package engine

import (
	"fmt"
	"sort"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"
)

// resourceBundle 记录一次调谐中新分配的全部资源，供“失败即回滚”使用。
// 所有布尔/集合字段保证回滚天然幂等：重复执行不会重复释放已回收资源。
type resourceBundle struct {
	quotaAcquired bool
	allocated     map[int]bool    // 端口 -> 是否已分配
	pids          map[string]int  // 服务名 -> 已启动进程 PID
	started       map[string]bool // 服务名 -> 是否已成功启动
	updatedStore  bool            // 环境快照是否已落盘（Ready/终态）
}

func newResourceBundle() *resourceBundle {
	return &resourceBundle{
		allocated: map[int]bool{},
		pids:      map[string]int{},
		started:   map[string]bool{},
	}
}

// rollback 逆序释放已分配资源。每个步骤自身幂等，错误被聚合为 RollbackError，
// 使回滚失败与正向失败可区分，并且调用方可安全重试。
func (e *Engine) rollback(envName string, b *resourceBundle, phase string, cause error) error {
	var errs []error
	if cause != nil {
		e.logf("rollback: env=%s phase=%s cause=%v", envName, phase, cause)
	}

	// 1) 先停进程（逆启动顺序）。
	svcNames := make([]string, 0, len(b.pids))
	for name := range b.started {
		svcNames = append(svcNames, name)
	}
	sort.Strings(svcNames)
	for i := len(svcNames) - 1; i >= 0; i-- {
		name := svcNames[i]
		pid := b.pids[name]
		if !b.started[name] {
			continue
		}
		if err := e.sandbox.Stop(envName, name, pid); err != nil {
			// 已经停止不算回滚失败（幂等）：跳过并记录。
			e.logf("rollback: stop %s/%s pid=%d -> %v (ignored if already stopped)", envName, name, pid, err)
		} else {
			e.logf("rollback: stopped process env=%s svc=%s pid=%d", envName, name, pid)
		}
		b.started[name] = false
	}

	// 2) 归还端口（逆序分配仅为日志可读，集合本身无序）。
	ports := make([]int, 0, len(b.allocated))
	for p := range b.allocated {
		ports = append(ports, p)
	}
	sort.Ints(ports)
	for i := len(ports) - 1; i >= 0; i-- {
		p := ports[i]
		if !b.allocated[p] {
			continue
		}
		if err := e.ports.Release(p); err != nil {
			errs = append(errs, fmt.Errorf("release port %d: %w", p, err))
		} else {
			e.logf("rollback: released port %d", p)
		}
		b.allocated[p] = false
	}

	// 3) 归还环境名额（可能转交给排队者）。
	if b.quotaAcquired {
		if err := e.releaseQuota(); err != nil {
			errs = append(errs, fmt.Errorf("release quota: %w", err))
		} else {
			e.logf("rollback: quota released env=%s", envName)
		}
		b.quotaAcquired = false
	}

	// 4) 沙箱目录清残留。
	if err := e.sandbox.RemoveEnvDir(envName); err != nil {
		errs = append(errs, fmt.Errorf("remove sandbox dir: %w", err))
	}

	if len(errs) > 0 {
		return &domain.RollbackError{Phase: phase, Errs: errs}
	}
	return nil
}

// resolveSecretRefs 解析单个服务的全部密钥引用，返回（环境变量名 -> 值）与
// 固化的版本绑定。任何缺失/失效都返回 SecretError，绝不退化为默认值。
//
// 环境变量名约定：SECRET_<密钥名大写>。
func (e *Engine) resolveSecretRefs(envName, svcName string, refs []domain.SecretRef) (map[string]string, []domain.SecretBinding, error) {
	values := map[string]string{}
	bindings := make([]domain.SecretBinding, 0, len(refs))
	for _, ref := range refs {
		val, err := e.secrets.Resolve(ref.Name, ref.Version)
		if err != nil {
			// 不记录 val，仅记录引用名与版本，保证密钥值不进入日志。
			return nil, nil, &domain.SecretError{
				Ref:    fmt.Sprintf("%s/%s/%s", envName, svcName, ref.Name),
				Reason: err.Error(),
			}
		}
		version := ref.Version
		if version == 0 {
			cur, cerr := e.secrets.Current(ref.Name)
			if cerr != nil {
				return nil, nil, &domain.SecretError{
					Ref:    fmt.Sprintf("%s/%s/%s", envName, svcName, ref.Name),
					Reason: cerr.Error(),
				}
			}
			version = cur
		}
		values["SECRET_"+upperName(ref.Name)] = val
		bindings = append(bindings, domain.SecretBinding{Name: ref.Name, Version: version})
	}
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].Name < bindings[j].Name })
	return values, bindings, nil
}

func upperName(s string) string {
	out := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		if (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			out[i] = c
		} else {
			out[i] = '_'
		}
	}
	return string(out)
}
