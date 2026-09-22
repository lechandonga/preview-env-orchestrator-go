package engine

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"
	"github.com/lechandonga/preview-env-orchestrator-go/internal/graph"
	"github.com/lechandonga/preview-env-orchestrator-go/internal/resources"
	"github.com/lechandonga/preview-env-orchestrator-go/internal/sandbox"
)

// Get 返回环境快照的深拷贝。
// 访问方永远拿不到“正在被销毁”的实例（Deleting/Terminated/不存在都返回 ok=false）。
func (e *Engine) Get(name string) (*domain.Environment, bool) {
	// 不获取环境锁：即使该环境正在调谐/排队/销毁，访问也必须立即得到确定答复。
	// Store 内部有读锁保护，返回的快照经过 Clone 与内部状态隔离。
	env, ok, err := e.store.Get(name)
	if err != nil || !ok {
		return nil, false
	}
	switch env.Phase {
	case domain.PhaseDeleting, domain.PhaseTerminated:
		return nil, false
	default:
		return env.Clone(), true
	}
}

// List 返回当前可访问（非销毁中）的全部环境快照。
func (e *Engine) List() ([]*domain.Environment, error) {
	envs, err := e.store.List()
	if err != nil {
		return nil, err
	}
	out := make([]*domain.Environment, 0, len(envs))
	for _, env := range envs {
		if env.Phase == domain.PhaseDeleting || env.Phase == domain.PhaseTerminated {
			continue
		}
		out = append(out, env.Clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Delete 销毁环境：标记 Deleting -> 逆拓扑停进程 -> 归还端口 -> 归还配额 ->
// 删除沙箱目录与状态快照。
//
// 并发/重试语义：
//   - 同一环境的并发 Delete 被环境锁串行化，第二个调用会发现快照已删除，
//     返回 ErrEnvNotFound 而不会重复释放资源；
//   - 进程停止/端口归还天然幂等，删除过程中途失败后再次 Delete 可安全继续；
//   - 释放期间 Get 返回不可见，访问方拿不到半销毁实例。
func (e *Engine) Delete(ctx context.Context, name string) error {
	fl, unlock := e.lockEnvFully(name)
	defer unlock()
	_ = fl

	env, ok, err := e.store.Get(name)
	if err != nil {
		return err
	}
	if !ok {
		return &ErrEnvNotFound{Name: name}
	}
	if env.Phase == domain.PhaseDeleting {
		// 上一次删除可能中断：继续执行幂等清理而不是报错。
		e.logf("delete: env=%s resume interrupted deletion", name)
	} else {
		env.Phase = domain.PhaseDeleting
		env.UpdatedAt = time.Now()
		if err := e.store.Put(env); err != nil {
			return err
		}
		e.logf("delete: env=%s marked Deleting", name)
	}

	if err := e.stopAndReleaseAll(name, env); err != nil {
		return err
	}

	// 仅当快照记录其持有名额时才归还（受控重建的更新不持有新名额；
	// 失败回滚已成功的残留快照 QuotaHeld=false，归还会超 draw）。
	if env.QuotaHeld {
		if err := e.releaseQuota(); err != nil && !errorsIs(err, resources.ErrQuotaOverdraw) {
			return err
		}
		e.logf("delete: quota released env=%s", name)
		env.QuotaHeld = false
	}

	if err := e.sandbox.RemoveEnvDir(name); err != nil {
		return fmt.Errorf("remove sandbox dir: %w", err)
	}
	if err := e.store.Delete(name); err != nil {
		return fmt.Errorf("remove snapshot: %w", err)
	}
	e.logf("delete: env=%s fully terminated", name)
	return nil
}

// stopAndReleaseAll 逆拓扑停止进程并归还全部端口；每一步幂等。
func (e *Engine) stopAndReleaseAll(name string, env *domain.Environment) error {
	svcNames := make([]string, 0, len(env.Services))
	for n := range env.Services {
		svcNames = append(svcNames, n)
	}
	// 依据依赖构造逆序：被依赖者后停。
	stopped := map[string]bool{}
	for {
		progress := false
		for _, n := range svcNames {
			if stopped[n] {
				continue
			}
			st := env.Services[n]
			readyToStop := true
			for _, dep := range st.Spec.DependsOn {
				if env.Services[dep] != nil && !stopped[dep] {
					readyToStop = false
					break
				}
			}
			if !readyToStop {
				continue
			}
			if st.PID > 0 {
				if err := e.sandbox.Stop(name, n, st.PID); err != nil {
					// 已停止/陈旧 PID 均视为可继续，但记录流水。
					e.logf("delete: stop %s/%s pid=%d -> %v", name, n, st.PID, err)
				} else {
					e.logf("delete: stopped process env=%s svc=%s pid=%d", name, n, st.PID)
				}
			}
			for _, pb := range st.Ports {
				if err := e.ports.Release(pb.Port); err != nil && !errorsIs(err, resources.ErrPortNotAllocated) {
					return fmt.Errorf("release port %d for %s: %w", pb.Port, n, err)
				}
				e.logf("delete: released port=%d svc=%s", pb.Port, n)
			}
			stopped[n] = true
			progress = true
		}
		if !progress {
			// 剩余服务（理论上只可能因环，环已在 Apply 拒绝），直接强制清理。
			for _, n := range svcNames {
				if stopped[n] {
					continue
				}
				st := env.Services[n]
				if st.PID > 0 {
					_ = e.sandbox.Stop(name, n, st.PID)
				}
				for _, pb := range st.Ports {
					_ = e.ports.Release(pb.Port)
				}
				stopped[n] = true
			}
			break
		}
	}
	return nil
}

// Reconcile 对单个环境对账：
//  1. 快照存在但进程被外部杀死 -> 按声明重新启动该服务（保持其绑定的密钥版本；
//     若绑定版本已被吊销，明确返回 NotConvergeableError 而不是退化）；
//  2. 发现沙箱中存在快照未记录的进程/PID 文件（孤儿）-> 清理并报告；
//  3. 快照丢失但沙箱目录残留 -> 记录并清理孤儿资源；
//  4. Deleting 状态 -> 继续完成删除。
func (e *Engine) Reconcile(ctx context.Context, name string) (*domain.Environment, error) {
	_, unlock := e.lockEnvFully(name)
	defer unlock()
	return e.reconcileLocked(ctx, name)
}

func (e *Engine) reconcileLocked(ctx context.Context, name string) (*domain.Environment, error) {
	env, ok, err := e.store.Get(name)
	if err != nil {
		return nil, err
	}
	if !ok {
		// 快照不存在：清理任何沙箱孤儿，不占用端口/配额簿记。
		pids, _ := e.sandbox.ListPIDs(name)
		for svc, pid := range pids {
			_ = e.sandbox.Stop(name, svc, pid)
			e.logf("reconcile: killed orphan process env=%s svc=%s pid=%d (no snapshot)", name, svc, pid)
		}
		_ = e.sandbox.RemoveEnvDir(name)
		return nil, &NotConvergeable{Env: name, Reason: "snapshot missing; orphan sandbox resources removed"}
	}
	if env.Phase == domain.PhaseDeleting {
		return nil, e.finishDeleteLocked(env)
	}

	order, err := graph.TopoOrder(env.Spec.Services)
	if err != nil {
		return nil, &domain.NotConvergeableError{Env: name, Reason: err.Error()}
	}

	// 快照记录 PID 与沙箱实际 PID 文件对账。
	diskPIDs, err := e.sandbox.ListPIDs(name)
	if err != nil {
		return nil, err
	}
	for svc, pid := range diskPIDs {
		if _, known := env.Services[svc]; !known {
			// 孤儿进程：快照里没有的服务。
			if err := e.sandbox.Stop(name, svc, pid); err != nil {
				e.logf("reconcile: orphan stop failed env=%s svc=%s: %v", name, svc, err)
			} else {
				e.logf("reconcile: killed orphan process env=%s svc=%s pid=%d", name, svc, pid)
			}
		}
	}

	svcByName := map[string]domain.ServiceSpec{}
	for _, svc := range env.Spec.Services {
		svcByName[svc.Name] = svc
	}

	changed := false
	blocked := map[string]bool{}
	for _, sname := range order {
		st := env.Services[sname]
		if st == nil {
			// 快照缺少服务记录：属于无法解释的偏离，报告不可收敛而非静默补造。
			return nil, &domain.NotConvergeableError{
				Env:    name,
				Reason: fmt.Sprintf("service %q present in spec but missing in runtime snapshot", sname),
			}
		}

		// 依赖传播仍然成立。
		for _, dep := range st.Spec.DependsOn {
			up := env.Services[dep]
			if up == nil || (up.Phase != domain.ServiceReady) {
				phase := domain.ServiceBlocked
				reason := fmt.Sprintf("upstream %q not ready", dep)
				if up != nil {
					reason = fmt.Sprintf("upstream %q is %s", dep, up.Phase)
				}
				if st.Phase != phase || st.Reason != reason {
					st.Phase = phase
					st.Reason = reason
					changed = true
				}
				blocked[sname] = true
			}
		}
		if blocked[sname] {
			continue
		}

		if st.Phase == domain.ServiceBlocked {
			// 上游恢复后解除阻塞：走重启流程。
			st.Phase = domain.ServicePending
			st.Reason = ""
			changed = true
		}

		if st.Phase == domain.ServiceReady && st.PID > 0 && e.sandbox.IsReady(st.PID) {
			continue // 实际与期望一致。
		}

		// 偏离：进程缺失/死亡。用绑定时固化的密钥版本重建，缺失/吊销则明确失败。
		svc := svcByName[sname]
		secretVals := map[string]string{}
		for _, bnd := range st.Secrets {
			val, rerr := e.secrets.Resolve(bnd.Name, bnd.Version)
			if rerr != nil {
				return nil, &domain.NotConvergeableError{
					Env:    name,
					Reason: fmt.Sprintf("cannot restart %q: bound secret %s v%d unavailable: %v", sname, bnd.Name, bnd.Version, rerr),
				}
			}
			secretVals["SECRET_"+upperName(bnd.Name)] = val
		}

		// 端口复用快照绑定：簿记中仍登记则直接复用；簿记缺失（崩溃后端口池
		// 状态无法判定等）才尝试重新登记；若该端口被其它环境占用则明确不可收敛，
		// 绝不静默换端口（地址是访问契约）。
		for _, pb := range st.Ports {
			if e.ports.IsAllocatedByAny(pb.Port) {
				continue // 本环境持有的绑定仍然有效
			}
			if _, aerr := e.ports.Allocate(pb.Port); aerr != nil {
				st.Phase = domain.ServiceFailed
				st.Reason = fmt.Sprintf("bound port %d unavailable: %v", pb.Port, aerr)
				changed = true
				blocked[sname] = true
				break
			}
			e.logf("reconcile: re-registered drifted port=%d svc=%s", pb.Port, sname)
		}
		if blocked[sname] {
			continue
		}

		req := sandbox.StartRequest{
			EnvName:   name,
			Service:   sname,
			Image:     svc.Image,
			Env:       svc.Env,
			SecretEnv: secretVals,
			Fail:      svc.Image == "fail",
		}
		pid, serr := e.sandbox.Start(req)
		if serr != nil {
			st.Phase = domain.ServiceFailed
			st.Reason = serr.Error()
			changed = true
			blocked[sname] = true
			e.logf("reconcile: restart failed env=%s svc=%s err=%v", name, sname, serr)
			continue
		}
		st.PID = pid
		st.Phase = domain.ServiceReady
		st.Reason = ""
		st.StartedAt = time.Now()
		changed = true
		e.logf("reconcile: restarted drifted process env=%s svc=%s pid=%d (bound secret versions preserved)", name, sname, pid)
	}

	switch {
	case len(blocked) > 0:
		if env.Phase != domain.PhaseDegraded {
			env.Phase = domain.PhaseDegraded
			env.Reason = "reconcile found blocked/failed services"
			changed = true
		}
	default:
		if env.Phase != domain.PhaseReady {
			env.Phase = domain.PhaseReady
			env.Reason = ""
			changed = true
		}
	}
	if changed {
		env.UpdatedAt = time.Now()
		if err := e.store.Put(env); err != nil {
			return nil, err
		}
		e.logf("reconcile: env=%s converged after drift repair phase=%s", name, env.Phase)
	} else {
		e.logf("reconcile: env=%s already converged phase=%s", name, env.Phase)
	}
	return env.Clone(), nil
}

// finishDeleteLocked 完成被中断的删除。
func (e *Engine) finishDeleteLocked(env *domain.Environment) error {
	name := env.Name
	if err := e.stopAndReleaseAll(name, env); err != nil {
		return err
	}
	if env.QuotaHeld {
		if err := e.releaseQuota(); err != nil && !errorsIs(err, resources.ErrQuotaOverdraw) {
			return err
		}
	}
	_ = e.sandbox.RemoveEnvDir(name)
	if err := e.store.Delete(name); err != nil {
		return err
	}
	e.logf("reconcile: interrupted deletion finished env=%s", name)
	return nil
}

// ReconcileAll 对全部环境对账，并回收 now 时刻已过期的临时环境。
// 过期回收使用与 Delete 相同的幂等路径；调用方可反复重试。
func (e *Engine) ReconcileAll(ctx context.Context, now time.Time) error {
	envs, err := e.store.List()
	if err != nil {
		return err
	}
	// 先处理过期（回收），再做存活对账，避免给马上要消失的环境做无用重启。
	for _, env := range envs {
		if !env.ExpiresAt.IsZero() && !now.Before(env.ExpiresAt) && env.Phase != domain.PhaseDeleting {
			e.logf("ttl: env=%s expired at=%s -> deleting", env.Name, env.ExpiresAt.Format(time.RFC3339))
			if err := e.Delete(ctx, env.Name); err != nil {
				if _, ok := err.(*ErrEnvNotFound); ok {
					continue
				}
				e.logf("ttl: env=%s delete failed (retryable): %v", env.Name, err)
			}
		}
	}
	envs, err = e.store.List()
	if err != nil {
		return err
	}
	var firstErr error
	for _, env := range envs {
		if _, err := e.Reconcile(ctx, env.Name); err != nil {
			e.logf("reconcile: env=%s error: %v", env.Name, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// ErrEnvNotFound 表示环境不存在（含重复销毁的幂等拒绝场景）。
type ErrEnvNotFound struct{ Name string }

func (e *ErrEnvNotFound) Error() string { return "environment not found: " + e.Name }

// NotConvergeable 是 domain.NotConvergeableError 的引擎侧别名便于引用。
type NotConvergeable = domain.NotConvergeableError
