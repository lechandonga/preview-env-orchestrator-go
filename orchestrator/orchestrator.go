// Package orchestrator 把声明式环境描述收敛为可运行的本地预览环境实例。
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lechandonga/preview-env-orchestrator-go/ports"
	"github.com/lechandonga/preview-env-orchestrator-go/sandbox"
	"github.com/lechandonga/preview-env-orchestrator-go/secrets"
	"github.com/lechandonga/preview-env-orchestrator-go/spec"
)

// Status 是环境生命周期状态。
type Status string

const (
	StatusPending    Status = "Pending"
	StatusStarting   Status = "Starting"
	StatusReady      Status = "Ready"
	StatusBlocked    Status = "Blocked" // 依赖失败导致阻塞
	StatusFailed     Status = "Failed"
	StatusDestroying Status = "Destroying"
	StatusDestroyed  Status = "Destroyed"
)

// errProcStart 标记"进程启动/就绪失败"，用于与资源类失败（端口、密钥）区分。
var errProcStart = errors.New("process failed to start or become ready")

// ServiceState 是单个服务的运行态。
type ServiceState struct {
	Name    string
	Running bool
	Blocked bool // 上游失败导致阻塞
	Failed  bool
	Ports   map[string]int
}

// Environment 是一个环境实例的可见视图。
type Environment struct {
	Name      string
	SpecKey   string
	Status    Status
	Services  map[string]*ServiceState
	CreatedAt time.Time
	ExpiresAt time.Time // 零值表示不过期
}

// Config 是可配置上限与策略。
type Config struct {
	MaxEnvs      int           // 单机同时存在的环境数上限
	PortMin      int           // 可用端口范围
	PortMax      int
	MaxService   int           // 单环境服务数配额
	WorkDir      string        // 沙箱根目录
	Queue        bool          // 达到上限时排队等待（true）还是拒绝（false）
	ReadyTimeout time.Duration // 进程就绪观察窗口
}

func (c *Config) normalize() {
	if c.MaxEnvs <= 0 {
		c.MaxEnvs = 8
	}
	if c.PortMin == 0 {
		c.PortMin = 20000
	}
	if c.PortMax <= c.PortMin {
		c.PortMax = c.PortMin + 200
	}
	if c.MaxService <= 0 {
		c.MaxService = 16
	}
	if c.WorkDir == "" {
		c.WorkDir = filepath.Join(os.TempDir(), "preview-env-orchestrator")
	}
	if c.ReadyTimeout <= 0 {
		c.ReadyTimeout = 100 * time.Millisecond
	}
}

// boundSecret 是创建/更新时绑定的密钥版本（轮换不影响已绑定环境）。
type boundSecret struct {
	key     string
	version int
}

// svcRuntime 是单个服务的内部运行态。
type svcRuntime struct {
	proc    *sandbox.Process
	ports   map[string]int         // 端口名 -> 端口
	secrets map[string]boundSecret // 环境变量名 -> 绑定
	svcKey  string
	blocked bool
	failed  bool
}

// envState 是内部可变状态。
type envState struct {
	env        *Environment
	spec       *spec.EnvSpec
	specKey    string
	sb         *sandbox.Sandbox
	svcs       map[string]*svcRuntime
	destroying bool
	cleanupErr error // 回滚/回收失败，待对账重试
}

// Orchestrator 管理环境生命周期。
type Orchestrator struct {
	cfg     Config
	secrets *secrets.Store
	ports   *ports.Allocator
	ledger  *Ledger

	mu    sync.Mutex
	envs  map[string]*envState
	slots chan struct{} // 环境数上限信号量
}

func New(cfg Config, sec *secrets.Store) *Orchestrator {
	cfg.normalize()
	if sec == nil {
		sec = secrets.NewStore()
	}
	return &Orchestrator{
		cfg:     cfg,
		secrets: sec,
		ports:   ports.New(cfg.PortMin, cfg.PortMax),
		ledger:  NewLedger(),
		envs:    map[string]*envState{},
		slots:   make(chan struct{}, cfg.MaxEnvs),
	}
}

// Ledger 返回资源流水（测试打印判定依据用）。
func (o *Orchestrator) Ledger() *Ledger { return o.ledger }

// Ports 返回端口分配器（测试断言用）。
func (o *Orchestrator) Ports() *ports.Allocator { return o.ports }

func newSvcRuntime(svc *spec.ServiceSpec) *svcRuntime {
	return &svcRuntime{
		ports:   map[string]int{},
		secrets: map[string]boundSecret{},
		svcKey:  spec.ServiceKey(svc),
	}
}

func (st *envState) setSvcView(name string, rt *svcRuntime) {
	st.env.Services[name] = &ServiceState{Name: name, Ports: rt.ports}
}

// recomputeStatusLocked 由服务状态推导环境状态。
func (st *envState) recomputeStatusLocked() {
	anyFailed, anyBlocked := false, false
	for _, rt := range st.svcs {
		if rt.failed {
			anyFailed = true
		}
		if rt.blocked {
			anyBlocked = true
		}
	}
	switch {
	case anyFailed:
		st.env.Status = StatusFailed
	case anyBlocked:
		st.env.Status = StatusBlocked
	default:
		st.env.Status = StatusReady
	}
}

// startServiceLocked 为单个服务分配端口、解析密钥、启动进程并等待就绪。
// 资源类失败返回包装了 ErrCapacity/ErrSecretRef 的错误；
// 进程类失败返回包装 errProcStart 的错误。
func (o *Orchestrator) startServiceLocked(st *envState, svc *spec.ServiceSpec, rt *svcRuntime, undo *[]func() error) error {
	envName := st.env.Name
	// 1. 端口
	for _, pr := range svc.Ports {
		p, err := o.ports.Allocate(envName, pr.Preferred)
		if err != nil {
			return fmt.Errorf("%w: service %s port %s: %v", ErrCapacity, svc.Name, pr.Name, err)
		}
		rt.ports[pr.Name] = p
		o.ledger.Append(Event{Kind: "alloc-port", Env: envName, Service: svc.Name,
			Detail: fmt.Sprintf("%s=%d", pr.Name, p)})
		port, name := p, pr.Name
		*undo = append(*undo, func() error {
			o.ports.Release(envName, port)
			o.ledger.Append(Event{Kind: "release-port", Env: envName, Service: svc.Name,
				Detail: fmt.Sprintf("%s=%d (rollback)", name, port)})
			return nil
		})
	}
	// 2. 密钥引用：绑定时解析并固定版本；缺失/版本失效 = 明确失败。
	envVars := []string{}
	for k, v := range svc.Env {
		envVars = append(envVars, k+"="+v)
	}
	for _, ref := range svc.SecretRefs {
		version := ref.Version
		if version == 0 {
			latest, err := o.secrets.Latest(ref.Key)
			if err != nil {
				return fmt.Errorf("%w: service %s: secret %q: %v", ErrSecretRef, svc.Name, ref.Key, err)
			}
			version = latest
		}
		val, err := o.secrets.Get(ref.Key, version)
		if err != nil {
			return fmt.Errorf("%w: service %s: secret %q v%d: %v", ErrSecretRef, svc.Name, ref.Key, version, err)
		}
		rt.secrets[ref.Env] = boundSecret{key: ref.Key, version: version}
		envVars = append(envVars, ref.Env+"="+val)
		// 流水只记录引用与版本，绝不记录值。
		o.ledger.Append(Event{Kind: "bind-secret", Env: envName, Service: svc.Name,
			Detail: fmt.Sprintf("%s@%d -> $%s", ref.Key, version, ref.Env)})
	}
	// 3. 端口注入为环境变量（PORT_<NAME>）。
	for name, p := range rt.ports {
		envVars = append(envVars, "PORT_"+strings.ToUpper(name)+"="+fmt.Sprint(p))
	}
	// 4. 启动进程
	proc, err := st.sb.Start(svc.Name, svc.Command, envVars)
	if err != nil {
		return fmt.Errorf("%w: service %s: %v", errProcStart, svc.Name, err)
	}
	rt.proc = proc
	o.ledger.Append(Event{Kind: "start-proc", Env: envName, Service: svc.Name,
		Detail: fmt.Sprintf("pid=%d", proc.PID())})
	*undo = append(*undo, func() error {
		proc.Stop()
		o.ledger.Append(Event{Kind: "stop-proc", Env: envName, Service: svc.Name,
			Detail: fmt.Sprintf("pid=%d (rollback)", proc.PID())})
		return nil
	})
	// 5. 就绪观察：窗口内退出视为失败（上游未就绪前不得启动下游）。
	deadline := time.Now().Add(o.cfg.ReadyTimeout)
	for time.Now().Before(deadline) {
		if !proc.Alive() {
			return fmt.Errorf("%w: service %s exited during readiness window", errProcStart, svc.Name)
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil
}

// stopServiceLocked 停止服务并释放其端口，幂等。
func (o *Orchestrator) stopServiceLocked(st *envState, name string) {
	rt := st.svcs[name]
	if rt == nil {
		return
	}
	if rt.proc != nil {
		rt.proc.Stop()
		o.ledger.Append(Event{Kind: "stop-proc", Env: st.env.Name, Service: name,
			Detail: fmt.Sprintf("pid=%d", rt.proc.PID())})
		rt.proc = nil
	}
	names := make([]string, 0, len(rt.ports))
	for pn := range rt.ports {
		names = append(names, pn)
	}
	sort.Strings(names)
	for _, pn := range names {
		p := rt.ports[pn]
		o.ports.Release(st.env.Name, p)
		o.ledger.Append(Event{Kind: "release-port", Env: st.env.Name, Service: name,
			Detail: fmt.Sprintf("%s=%d", pn, p)})
	}
	rt.ports = map[string]int{}
	rt.secrets = map[string]boundSecret{}
}

// runUndo 逆序执行回滚步骤，聚合错误。
func (o *Orchestrator) runUndo(envName string, undo []func() error) error {
	var errs []error
	for i := len(undo) - 1; i >= 0; i-- {
		if err := undo[i](); err != nil {
			errs = append(errs, err)
			o.ledger.Append(Event{Kind: "rollback-failed", Env: envName, Detail: err.Error()})
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%w: %v", ErrRollbackFailed, errors.Join(errs...))
	}
	return nil
}

// createLocked 创建环境；资源类失败回滚全部已分配资源。
func (o *Orchestrator) createLocked(s *spec.EnvSpec, key string) (*envState, error) {
	st := &envState{
		spec:    s,
		specKey: key,
		svcs:    map[string]*svcRuntime{},
		env: &Environment{
			Name:      s.Name,
			SpecKey:   key,
			Status:    StatusStarting,
			Services:  map[string]*ServiceState{},
			CreatedAt: time.Now(),
		},
	}
	if s.TTL > 0 {
		st.env.ExpiresAt = st.env.CreatedAt.Add(s.TTL)
	}
	var undo []func() error
	sb, err := sandbox.New(filepath.Join(o.cfg.WorkDir, s.Name))
	if err != nil {
		return nil, fmt.Errorf("create sandbox: %v", err)
	}
	st.sb = sb
	undo = append(undo, func() error {
		if err := sb.Cleanup(); err != nil {
			return err
		}
		o.ledger.Append(Event{Kind: "cleanup-sandbox", Env: s.Name, Detail: "(rollback)"})
		return nil
	})

	svcByName := map[string]*spec.ServiceSpec{}
	for i := range s.Services {
		svcByName[s.Services[i].Name] = &s.Services[i]
	}
	order, err := spec.TopoOrder(s)
	if err != nil {
		o.runUndo(s.Name, undo)
		return nil, fmt.Errorf("%w: %v", ErrDependencyCycle, err)
	}
	depFailed := map[string]bool{}
	for _, name := range order {
		svc := svcByName[name]
		rt := newSvcRuntime(svc)
		st.svcs[name] = rt
		st.setSvcView(name, rt)
		blocked := false
		for _, d := range svc.DependsOn {
			if depFailed[d] {
				blocked = true
			}
		}
		if blocked {
			rt.blocked = true
			st.env.Services[name].Blocked = true
			depFailed[name] = true
			o.ledger.Append(Event{Kind: "svc-blocked", Env: s.Name, Service: name,
				Detail: "upstream failed"})
			continue
		}
		if err := o.startServiceLocked(st, svc, rt, &undo); err != nil {
			if errors.Is(err, errProcStart) {
				// 进程失败：服务标记失败，下游阻塞，环境保留可观测。
				rt.failed = true
				st.env.Services[name].Failed = true
				depFailed[name] = true
				o.ledger.Append(Event{Kind: "svc-failed", Env: s.Name, Service: name,
					Detail: err.Error()})
				continue
			}
			// 资源类失败：回滚全部已分配资源。
			if rbErr := o.runUndo(s.Name, undo); rbErr != nil {
				// 回滚自身失败：保留环境用于对账重试，错误可区分。
				st.cleanupErr = rbErr
				st.env.Status = StatusFailed
				o.envs[s.Name] = st
				return st, rbErr
			}
			return nil, err
		}
		st.env.Services[name].Running = true
	}
	st.recomputeStatusLocked()
	o.envs[s.Name] = st
	o.ledger.Append(Event{Kind: "create", Env: s.Name, Detail: "status=" + string(st.env.Status)})
	return st, nil
}

// updateLocked 把已有环境收敛到新描述：移除多余服务、替换变化服务、保留未变服务。
func (o *Orchestrator) updateLocked(st *envState, s *spec.EnvSpec, key string) error {
	envName := st.env.Name
	newByName := map[string]*spec.ServiceSpec{}
	for i := range s.Services {
		newByName[s.Services[i].Name] = &s.Services[i]
	}
	// 1. 停止被移除或已变化的服务。
	names := make([]string, 0, len(st.svcs))
	for name := range st.svcs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		ns, ok := newByName[name]
		if !ok || spec.ServiceKey(ns) != st.svcs[name].svcKey {
			o.stopServiceLocked(st, name)
			delete(st.svcs, name)
			delete(st.env.Services, name)
			o.ledger.Append(Event{Kind: "svc-remove", Env: envName, Service: name})
		}
	}
	// 2. 按拓扑序确保各服务运行。
	order, err := spec.TopoOrder(s)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrDependencyCycle, err)
	}
	depFailed := map[string]bool{}
	for _, name := range order {
		svc := newByName[name]
		rt := st.svcs[name]
		blocked := false
		for _, d := range svc.DependsOn {
			if depFailed[d] {
				blocked = true
			}
		}
		if blocked {
			if rt == nil {
				rt = newSvcRuntime(svc)
				st.svcs[name] = rt
				st.setSvcView(name, rt)
			}
			rt.blocked = true
			st.env.Services[name].Blocked = true
			depFailed[name] = true
			o.ledger.Append(Event{Kind: "svc-blocked", Env: envName, Service: name,
				Detail: "upstream failed"})
			continue
		}
		if rt != nil && rt.proc != nil && rt.proc.Alive() {
			continue // 未变化且运行中：保留，不追加副作用。
		}
		rt = newSvcRuntime(svc)
		st.svcs[name] = rt
		st.setSvcView(name, rt)
		var undo []func() error
		if err := o.startServiceLocked(st, svc, rt, &undo); err != nil {
			o.runUndo(envName, undo) // 尽力回滚本次变更
			rt.failed = true
			st.env.Services[name].Failed = true
			depFailed[name] = true
			o.ledger.Append(Event{Kind: "svc-failed", Env: envName, Service: name,
				Detail: err.Error()})
			if !errors.Is(err, errProcStart) {
				st.recomputeStatusLocked()
				return err // 资源类失败：明确返回，环境保留旧运行服务
			}
			continue
		}
		st.env.Services[name].Running = true
	}
	st.spec = s
	st.specKey = key
	st.env.SpecKey = key
	st.env.ExpiresAt = time.Time{}
	if s.TTL > 0 {
		st.env.ExpiresAt = time.Now().Add(s.TTL)
	}
	st.recomputeStatusLocked()
	o.ledger.Append(Event{Kind: "update", Env: envName, Detail: "status=" + string(st.env.Status)})
	return nil
}

// acquireSlot 按策略占用环境槽位：Queue=true 排队等待，否则拒绝。
func (o *Orchestrator) acquireSlot(ctx context.Context) error {
	if o.cfg.Queue {
		select {
		case o.slots <- struct{}{}:
			return nil
		case <-ctx.Done():
			return fmt.Errorf("%w: %v", ErrCapacity, ctx.Err())
		}
	}
	select {
	case o.slots <- struct{}{}:
		return nil
	default:
		return fmt.Errorf("%w: max environments %d reached", ErrCapacity, o.cfg.MaxEnvs)
	}
}

func (o *Orchestrator) releaseSlot() { <-o.slots }

// Apply 幂等地创建或更新环境：同一描述重复提交得到同一实例。
func (o *Orchestrator) Apply(ctx context.Context, s *spec.EnvSpec) (*Environment, error) {
	if err := spec.Validate(s); err != nil {
		var ce *spec.CycleError
		if errors.As(err, &ce) {
			return nil, fmt.Errorf("%w: %v", ErrDependencyCycle, err)
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidSpec, err)
	}
	if len(s.Services) > o.cfg.MaxService {
		return nil, fmt.Errorf("%w: service quota %d exceeded", ErrCapacity, o.cfg.MaxService)
	}
	key := s.Key()

	o.mu.Lock()
	if st, ok := o.envs[s.Name]; ok {
		if st.destroying {
			o.mu.Unlock()
			return nil, fmt.Errorf("%w: %s", ErrDestroying, s.Name)
		}
		if st.specKey == key {
			env := cloneEnv(st.env)
			o.mu.Unlock()
			return env, nil // 幂等：同一描述得到同一实例
		}
		err := o.updateLocked(st, s, key)
		env := cloneEnv(st.env)
		o.mu.Unlock()
		return env, err
	}
	o.mu.Unlock()

	if err := o.acquireSlot(ctx); err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	// 双重检查：等待槽位期间可能已被并发创建。
	if st, ok := o.envs[s.Name]; ok {
		o.releaseSlot()
		if st.destroying {
			return nil, fmt.Errorf("%w: %s", ErrDestroying, s.Name)
		}
		if st.specKey == key {
			return cloneEnv(st.env), nil
		}
		err := o.updateLocked(st, s, key)
		return cloneEnv(st.env), err
	}
	st, err := o.createLocked(s, key)
	if err != nil && st == nil {
		o.releaseSlot()
		return nil, err
	}
	if err != nil {
		// 回滚失败：环境保留（Failed + cleanupErr），槽位不释放，待对账重试。
		return cloneEnv(st.env), err
	}
	return cloneEnv(st.env), nil
}

// Get 返回环境实例；正在销毁的环境对访问方不可见。
func (o *Orchestrator) Get(name string) (*Environment, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	st, ok := o.envs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	if st.destroying {
		return nil, fmt.Errorf("%w: %s", ErrDestroying, name)
	}
	return cloneEnv(st.env), nil
}

// teardown 停止进程、释放端口、清理沙箱；幂等，可安全重试。
func (o *Orchestrator) teardown(st *envState) error {
	var errs []error
	names := make([]string, 0, len(st.svcs))
	for name := range st.svcs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		o.stopServiceLocked(st, name)
	}
	if st.sb != nil {
		if err := st.sb.Cleanup(); err != nil {
			errs = append(errs, err)
			o.ledger.Append(Event{Kind: "cleanup-failed", Env: st.env.Name, Detail: err.Error()})
		} else {
			o.ledger.Append(Event{Kind: "cleanup-sandbox", Env: st.env.Name})
		}
	}
	return errors.Join(errs...)
}

// Destroy 回收环境，幂等且并发安全。
func (o *Orchestrator) Destroy(name string) error {
	o.mu.Lock()
	st, ok := o.envs[name]
	if !ok {
		o.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	if st.destroying {
		// 另一个调用者正在回收；回收操作幂等，直接返回。
		o.mu.Unlock()
		return nil
	}
	st.destroying = true
	st.env.Status = StatusDestroying
	err := o.teardown(st)
	if err != nil {
		// 回收失败：保持环境可重试，不释放槽位。
		st.destroying = false
		st.cleanupErr = err
		st.env.Status = StatusFailed
		o.mu.Unlock()
		return fmt.Errorf("%w: %v", ErrRollbackFailed, err)
	}
	delete(o.envs, name)
	st.env.Status = StatusDestroyed
	o.releaseSlot()
	o.mu.Unlock()
	o.ledger.Append(Event{Kind: "destroy", Env: name})
	return nil
}

// Reconcile 对账：重试失败的清理、修复外部漂移，或明确报告不可收敛。
func (o *Orchestrator) Reconcile(name string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	st, ok := o.envs[name]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	// 1. 重试此前失败的回滚/回收，不重复释放已回收资源（各步骤幂等）。
	if st.cleanupErr != nil {
		if err := o.teardown(st); err != nil {
			st.cleanupErr = err
			return fmt.Errorf("%w: %v", ErrRollbackFailed, err)
		}
		st.cleanupErr = nil
		delete(o.envs, name)
		st.env.Status = StatusDestroyed
		o.releaseSlot()
		o.ledger.Append(Event{Kind: "reconcile", Env: name, Detail: "cleanup retry succeeded"})
		return nil
	}
	if st.destroying {
		return fmt.Errorf("%w: %s", ErrDestroying, name)
	}
	// 2. 检查实际状态与期望状态的偏离。
	drift := []string{}
	for svcName, rt := range st.svcs {
		if rt.failed || rt.blocked {
			continue
		}
		if rt.proc != nil && !rt.proc.Alive() {
			drift = append(drift, "proc-dead:"+svcName)
		}
		for pn, p := range rt.ports {
			if owner, ok := o.ports.Owner(p); !ok || owner != name {
				drift = append(drift, "port-lost:"+svcName+":"+pn)
			}
		}
	}
	if len(drift) == 0 {
		o.ledger.Append(Event{Kind: "reconcile", Env: name, Detail: "converged"})
		return nil
	}
	o.ledger.Append(Event{Kind: "reconcile", Env: name, Detail: "drift=" + strings.Join(drift, ",")})
	// 3. 修复：重启死亡进程；端口丢失则重新分配。
	svcByName := map[string]*spec.ServiceSpec{}
	for i := range st.spec.Services {
		svcByName[st.spec.Services[i].Name] = &st.spec.Services[i]
	}
	for _, d := range drift {
		parts := strings.SplitN(d, ":", 2)
		svcName := parts[1]
		rt := st.svcs[svcName]
		svc := svcByName[svcName]
		switch parts[0] {
		case "proc-dead":
			o.stopServiceLocked(st, svcName)
			nrt := newSvcRuntime(svc)
			st.svcs[svcName] = nrt
			st.setSvcView(svcName, nrt)
			var undo []func() error
			if err := o.startServiceLocked(st, svc, nrt, &undo); err != nil {
				o.runUndo(name, undo)
				nrt.failed = true
				st.env.Services[svcName].Failed = true
				st.recomputeStatusLocked()
				if errors.Is(err, errProcStart) {
					o.ledger.Append(Event{Kind: "svc-failed", Env: name, Service: svcName, Detail: err.Error()})
					continue
				}
				return fmt.Errorf("%w: restart %s: %v", ErrNotConvergent, svcName, err)
			}
			st.env.Services[svcName].Running = true
			o.ledger.Append(Event{Kind: "reconcile-fix", Env: name, Service: svcName, Detail: "restarted"})
		case "port-lost":
			pn := parts[2]
			delete(rt.ports, pn)
			preferred := 0
			for _, pr := range svc.Ports {
				if pr.Name == pn {
					preferred = pr.Preferred
				}
			}
			p, err := o.ports.Allocate(name, preferred)
			if err != nil {
				return fmt.Errorf("%w: realloc port %s/%s: %v", ErrNotConvergent, svcName, pn, err)
			}
			rt.ports[pn] = p
			o.ledger.Append(Event{Kind: "reconcile-fix", Env: name, Service: svcName,
				Detail: fmt.Sprintf("realloc %s=%d", pn, p)})
		}
	}
	st.recomputeStatusLocked()
	return nil
}

// ReapExpired 回收到期环境；失败保留待下次重试，不误删未到期环境。
func (o *Orchestrator) ReapExpired(now time.Time) {
	o.mu.Lock()
	var expired []string
	for name, st := range o.envs {
		if st.destroying || st.env.ExpiresAt.IsZero() {
			continue
		}
		if !now.Before(st.env.ExpiresAt) {
			expired = append(expired, name)
		}
	}
	o.mu.Unlock()
	for _, name := range expired {
		if err := o.Destroy(name); err != nil {
			o.ledger.Append(Event{Kind: "reap-failed", Env: name, Detail: err.Error()})
			continue
		}
		o.ledger.Append(Event{Kind: "reap", Env: name})
	}
}

// List 返回全部环境名（不含已销毁）。
func (o *Orchestrator) List() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	names := make([]string, 0, len(o.envs))
	for n := range o.envs {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Close 回收全部环境。
func (o *Orchestrator) Close() {
	for _, name := range o.List() {
		o.Destroy(name)
	}
}

func cloneEnv(e *Environment) *Environment {
	c := *e
	c.Services = map[string]*ServiceState{}
	for n, s := range e.Services {
		sc := *s
		sc.Ports = map[string]int{}
		for pn, p := range s.Ports {
			sc.Ports[pn] = p
		}
		c.Services[n] = &sc
	}
	return &c
}
