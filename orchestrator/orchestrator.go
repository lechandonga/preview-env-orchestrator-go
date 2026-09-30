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

// ServiceState 是单个服务的可见运行态。
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
	MaxEnvs      int // 全局同时存在的环境数上限（跨实例共享）
	PortMin      int // 可用端口范围
	PortMax      int
	MaxService   int           // 单环境服务数配额
	WorkDir      string        // 沙箱根目录
	StateDir     string        // 落盘状态目录；为空时取 WorkDir/state
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
	if c.StateDir == "" {
		c.StateDir = filepath.Join(c.WorkDir, "state")
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
	pid     int // 接管进程的 pid（proc 不可用时的回退信息）
	ports   map[string]int
	secrets map[string]boundSecret // 环境变量名 -> 绑定
	svcKey  string
	blocked bool
	failed  bool
}

// envState 是内部可变状态（每次从落盘记录重建）。
type envState struct {
	rec        *envRecord
	env        *Environment
	spec       *spec.EnvSpec
	specKey    string
	sb         *sandbox.Sandbox
	svcs       map[string]*svcRuntime
	destroying bool
	cleanupErr error // 回滚/回收失败，待对账重试
}

// Orchestrator 管理环境生命周期；多个实例可共用同一份 StateDir。
type Orchestrator struct {
	cfg     Config
	secrets *secrets.Store
	ports   *ports.Allocator
	ledger  *Ledger

	lock *fileLock // 跨进程互斥（flock）

	mu   sync.Mutex // 本进程内互斥
	envs map[string]*envState

	cleanupHook func(envName string) error // 测试钩子：注入清理失败
}

// New 打开（或初始化）一份引擎状态。指向同一 StateDir 的多个实例共享
// 环境、端口与配额状态；重启进程后调用 New 即完成接管。
func New(cfg Config, sec *secrets.Store) (*Orchestrator, error) {
	cfg.normalize()
	if sec == nil {
		sec = secrets.NewStore()
	}
	if err := os.MkdirAll(cfg.StateDir, 0o755); err != nil {
		return nil, fmt.Errorf("init state dir: %w", err)
	}
	if err := os.MkdirAll(cfg.WorkDir, 0o755); err != nil {
		return nil, fmt.Errorf("init work dir: %w", err)
	}
	fl, err := newFileLock(filepath.Join(cfg.StateDir, lockFileName))
	if err != nil {
		return nil, err
	}
	pa := ports.New(cfg.PortMin, cfg.PortMax)
	portsPath := filepath.Join(cfg.StateDir, portsFileName)
	if err := pa.EnsureFile(portsPath); err != nil {
		return nil, err
	}
	o := &Orchestrator{
		cfg:     cfg,
		secrets: sec,
		ports:   pa,
		ledger:  NewLedger(),
		lock:    fl,
		envs:    map[string]*envState{},
	}
	ledgerPath := filepath.Join(cfg.StateDir, ledgerFileName)
	o.ledger.setSink(loadLedgerFile(ledgerPath), func(e Event) {
		_ = appendLedgerFile(ledgerPath, e)
	})
	// 初始化期间持独占锁加载并修复崩溃残留。
	if err := fl.lock(true); err != nil {
		return nil, err
	}
	if err := o.reloadLocked(); err != nil {
		fl.unlock()
		return nil, err
	}
	fl.unlock()
	return o, nil
}

// Ledger 返回资源流水（测试打印判定依据用）。
func (o *Orchestrator) Ledger() *Ledger { return o.ledger }

// Ports 返回端口分配器（测试断言用）。
func (o *Orchestrator) Ports() *ports.Allocator { return o.ports }

func (o *Orchestrator) portsPath() string  { return filepath.Join(o.cfg.StateDir, portsFileName) }
func (o *Orchestrator) savePorts() error   { return o.ports.Save(o.portsPath()) }
func (o *Orchestrator) ledgerPath() string { return filepath.Join(o.cfg.StateDir, ledgerFileName) }

// withLock 持有跨进程锁与进程内锁执行 fn。exclusive=false 用于只读操作。
func (o *Orchestrator) withLock(exclusive bool, reload bool, fn func() error) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.lock.lock(exclusive); err != nil {
		return err
	}
	defer o.lock.unlock()
	if reload {
		if err := o.reloadLocked(); err != nil {
			return err
		}
	}
	return fn()
}

func newSvcRuntime(svc *spec.ServiceSpec) *svcRuntime {
	return &svcRuntime{
		ports:   map[string]int{},
		secrets: map[string]boundSecret{},
		svcKey:  spec.ServiceKey(svc),
	}
}

func (st *envState) setSvcView(name string, rt *svcRuntime) {
	alive := rt.proc != nil && rt.proc.Alive()
	st.env.Services[name] = &ServiceState{
		Name:    name,
		Running: alive,
		Blocked: rt.blocked,
		Failed:  rt.failed,
		Ports:   rt.ports,
	}
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

// recordLocked 把内存状态序列化为落盘记录（不含任何密钥值）。
func (o *Orchestrator) recordLocked(st *envState) *envRecord {
	r := &envRecord{
		Version:    stateVersion,
		Name:       st.env.Name,
		Spec:       *st.spec,
		SpecKey:    st.specKey,
		Phase:      phaseActive,
		Status:     st.env.Status,
		CreatedAt:  st.env.CreatedAt,
		ExpiresAt:  st.env.ExpiresAt,
		Services:   nil,
		CleanupErr: "",
	}
	if st.destroying {
		r.Phase = phaseDestroying
	}
	if st.cleanupErr != nil {
		r.CleanupErr = st.cleanupErr.Error()
	}
	names := make([]string, 0, len(st.svcs))
	for n := range st.svcs {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		rt := st.svcs[n]
		sr := svcRecord{
			Name:    n,
			SvcKey:  rt.svcKey,
			Ports:   map[string]int{},
			Secrets: map[string]boundSecretRef{},
			Blocked: rt.blocked,
			Failed:  rt.failed,
		}
		if rt.proc != nil && rt.proc.Alive() {
			sr.PID = rt.proc.PID()
		} else if rt.pid > 0 {
			sr.PID = rt.pid
		}
		for pn, p := range rt.ports {
			sr.Ports[pn] = p
		}
		for envName, b := range rt.secrets {
			sr.Secrets[envName] = boundSecretRef{Key: b.key, Version: b.version}
		}
		r.Services = append(r.Services, sr)
	}
	return r
}

// persistLocked 原子写入环境记录。
func (o *Orchestrator) persistLocked(st *envState) error {
	return saveRecord(o.cfg.StateDir, o.recordLocked(st))
}

// reloadLocked 从落盘状态重建内存视图并执行启动接管：
// 加载端口分配、接管仍在运行的进程、清理崩溃留下的半截环境。
// 调用方必须已持有跨进程锁。
func (o *Orchestrator) reloadLocked() error {
	if err := o.ports.Load(o.portsPath()); err != nil {
		return fmt.Errorf("load ports: %w", err)
	}
	records, err := loadRecords(o.cfg.StateDir)
	if err != nil {
		return fmt.Errorf("load state: %w", err)
	}
	o.envs = map[string]*envState{}
	for _, r := range records {
		switch r.Phase {
		case phaseCreating:
			// 崩溃在创建过程中：回滚端口、停残留进程、清沙箱与记录。
			o.abandonOrphanLocked(r)
			continue
		case phaseDestroying:
			// 崩溃在销毁过程中：把未完成的销毁做完（步骤幂等）。
			o.resumeDestroyLocked(r)
			continue
		}
		st, err := o.buildStateLocked(r)
		if err != nil {
			return err
		}
		o.envs[r.Name] = st
	}
	return nil
}

// abandonOrphanLocked 清理崩溃在 creating 阶段的环境。
func (o *Orchestrator) abandonOrphanLocked(r *envRecord) {
	for _, sr := range r.Services {
		if sr.PID > 0 {
			if proc, err := sandbox.Attach(sr.Name, sr.PID); err == nil {
				proc.Stop()
			}
		}
		for pn, p := range sr.Ports {
			o.ports.Release(r.Name, p)
			o.ledger.Append(Event{Kind: "release-port", Env: r.Name, Service: sr.Name,
				Detail: fmt.Sprintf("%s=%d (orphan-create)", pn, p)})
		}
	}
	_ = os.RemoveAll(filepath.Join(o.cfg.WorkDir, r.Name))
	_ = deleteRecord(o.cfg.StateDir, r.Name)
	_ = o.savePorts()
	o.ledger.Append(Event{Kind: "orphan-abandoned", Env: r.Name,
		Detail: "crashed during create"})
}

// resumeDestroyLocked 完成崩溃在 destroying 阶段的环境清理。
func (o *Orchestrator) resumeDestroyLocked(r *envRecord) {
	for _, sr := range r.Services {
		if sr.PID > 0 {
			if proc, err := sandbox.Attach(sr.Name, sr.PID); err == nil {
				proc.Stop()
			}
		}
		for pn, p := range sr.Ports {
			o.ports.Release(r.Name, p)
			o.ledger.Append(Event{Kind: "release-port", Env: r.Name, Service: sr.Name,
				Detail: fmt.Sprintf("%s=%d (destroy-resume)", pn, p)})
		}
	}
	_ = os.RemoveAll(filepath.Join(o.cfg.WorkDir, r.Name))
	_ = deleteRecord(o.cfg.StateDir, r.Name)
	_ = o.savePorts()
	o.ledger.Append(Event{Kind: "destroy-resume", Env: r.Name,
		Detail: "crashed during destroy; cleanup completed"})
}

// buildStateLocked 由落盘记录重建内存状态，并接管仍存活的服务进程。
func (o *Orchestrator) buildStateLocked(r *envRecord) (*envState, error) {
	s := r.Spec
	st := &envState{
		rec:     r,
		spec:    &s,
		specKey: r.SpecKey,
		sb:      sandbox.AttachRoot(filepath.Join(o.cfg.WorkDir, r.Name)),
		svcs:    map[string]*svcRuntime{},
		env: &Environment{
			Name:      r.Name,
			SpecKey:   r.SpecKey,
			Status:    r.Status,
			Services:  map[string]*ServiceState{},
			CreatedAt: r.CreatedAt,
			ExpiresAt: r.ExpiresAt,
		},
	}
	if r.CleanupErr != "" {
		st.cleanupErr = errors.New(r.CleanupErr)
	}
	for i := range r.Services {
		sr := r.Services[i]
		rt := &svcRuntime{
			pid:     sr.PID,
			ports:   map[string]int{},
			secrets: map[string]boundSecret{},
			svcKey:  sr.SvcKey,
			blocked: sr.Blocked,
			failed:  sr.Failed,
		}
		for pn, p := range sr.Ports {
			rt.ports[pn] = p
		}
		// 只恢复引用，不接触密钥值；实际取值发生在（重新）启动时。
		for envName, ref := range sr.Secrets {
			rt.secrets[envName] = boundSecret{key: ref.Key, version: ref.Version}
		}
		if sr.PID > 0 {
			if proc, err := sandbox.Attach(sr.Name, sr.PID); err == nil {
				rt.proc = proc
				rt.pid = sr.PID
			} else {
				// 进程已不存在：保留 pid 记录，视为外部漂移，由 Reconcile 修复。
				rt.pid = sr.PID
				o.ledger.Append(Event{Kind: "reattach-lost", Env: r.Name, Service: sr.Name,
					Detail: fmt.Sprintf("pid=%d gone; awaiting reconcile", sr.PID)})
			}
		}
		st.svcs[sr.Name] = rt
		st.setSvcView(sr.Name, rt)
	}
	if st.cleanupErr != nil {
		st.env.Status = StatusFailed // 清理失败的环境保持 Failed，不被服务态推导覆盖
	} else {
		st.recomputeStatusLocked()
	}
	return st, nil
}

// resolveSecretLocked 解析密钥：优先使用绑定时固定的版本，从不静默改用最新版本。
// 缺失/被吊销/版本失效一律明确失败（ErrSecretRef）。
func (o *Orchestrator) resolveSecretLocked(envName, svcName string, ref spec.SecretRef, rt *svcRuntime) (string, boundSecret, error) {
	version := ref.Version
	if b, ok := rt.secrets[ref.Env]; ok && b.version > 0 {
		// 重启/修复路径：坚持原绑定版本。
		version = b.version
	}
	if version == 0 {
		latest, err := o.secrets.Latest(ref.Key)
		if err != nil {
			return "", boundSecret{}, fmt.Errorf("%w: service %s: secret %q: %w", ErrSecretRef, svcName, ref.Key, err)
		}
		version = latest
	}
	val, err := o.secrets.Get(ref.Key, version)
	if err != nil {
		return "", boundSecret{}, fmt.Errorf("%w: service %s: secret %q v%d: %w", ErrSecretRef, svcName, ref.Key, version, err)
	}
	b := boundSecret{key: ref.Key, version: version}
	rt.secrets[ref.Env] = b
	o.ledger.Append(Event{Kind: "bind-secret", Env: envName, Service: svcName,
		Detail: fmt.Sprintf("%s@%d -> $%s", ref.Key, version, ref.Env)})
	return val, b, nil
}

// startServiceLocked 为单个服务分配端口、解析密钥、启动进程并等待就绪。
// 资源类失败返回包装了 ErrCapacity/ErrSecretRef 的错误；
// 进程类失败返回包装 errProcStart 的错误。
func (o *Orchestrator) startServiceLocked(st *envState, svc *spec.ServiceSpec, rt *svcRuntime, undo *[]func() error) error {
	envName := st.env.Name
	if st.sb == nil {
		sb, err := sandbox.New(filepath.Join(o.cfg.WorkDir, envName))
		if err != nil {
			return fmt.Errorf("create sandbox: %v", err)
		}
		st.sb = sb
	}
	// 1. 端口：先更新落盘的端口表，再进行后续动作（崩溃后端口也不会被重复分配）。
	for _, pr := range svc.Ports {
		p, err := o.ports.Allocate(envName, pr.Preferred)
		if err != nil {
			return fmt.Errorf("%w: service %s port %s: %v", ErrCapacity, svc.Name, pr.Name, err)
		}
		rt.ports[pr.Name] = p
		if err := o.savePorts(); err != nil {
			o.ports.Release(envName, p)
			delete(rt.ports, pr.Name)
			return fmt.Errorf("persist ports: %v", err)
		}
		o.ledger.Append(Event{Kind: "alloc-port", Env: envName, Service: svc.Name,
			Detail: fmt.Sprintf("%s=%d", pr.Name, p)})
		port, name := p, pr.Name
		*undo = append(*undo, func() error {
			if o.ports.Release(envName, port) {
				o.savePorts()
				o.ledger.Append(Event{Kind: "release-port", Env: envName, Service: svc.Name,
					Detail: fmt.Sprintf("%s=%d (rollback)", name, port)})
			}
			return nil
		})
	}
	// 2. 密钥引用：绑定时解析并固定版本；缺失/版本失效 = 明确失败，绝不退化为最新版本。
	envVars := []string{}
	for k, v := range svc.Env {
		envVars = append(envVars, k+"="+v)
	}
	for _, ref := range svc.SecretRefs {
		val, _, err := o.resolveSecretLocked(envName, svc.Name, ref, rt)
		if err != nil {
			return err
		}
		envVars = append(envVars, ref.Env+"="+val)
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
	rt.pid = proc.PID()
	if err := o.persistLocked(st); err != nil {
		proc.Stop()
		rt.proc = nil
		return fmt.Errorf("persist state: %v", err)
	}
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

// stopServiceLocked 停止服务并释放其端口，幂等（跨重启也只生效一次）。
func (o *Orchestrator) stopServiceLocked(st *envState, name string, reason string) {
	rt := st.svcs[name]
	if rt == nil {
		return
	}
	if rt.proc != nil {
		pid := rt.proc.PID()
		rt.proc.Stop()
		o.ledger.Append(Event{Kind: "stop-proc", Env: st.env.Name, Service: name,
			Detail: fmt.Sprintf("pid=%d%s", pid, reason)})
		rt.proc = nil
	}
	rt.pid = 0
	names := make([]string, 0, len(rt.ports))
	for pn := range rt.ports {
		names = append(names, pn)
	}
	sort.Strings(names)
	releasedAny := false
	for _, pn := range names {
		p := rt.ports[pn]
		if o.ports.Release(st.env.Name, p) {
			releasedAny = true
			o.ledger.Append(Event{Kind: "release-port", Env: st.env.Name, Service: name,
				Detail: fmt.Sprintf("%s=%d%s", pn, p, reason)})
		}
	}
	if releasedAny {
		o.savePorts()
	}
	rt.ports = map[string]int{}
	// 注意：secrets 绑定保留，修复/对账仍需坚持原版本。
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
	st.rec = o.recordLocked(st)
	st.rec.Phase = phaseCreating
	// 先落盘 creating 标记：本进程在此之后崩溃，重启时该记录被识别为孤儿并清理。
	if err := saveRecord(o.cfg.StateDir, st.rec); err != nil {
		return nil, fmt.Errorf("persist state: %v", err)
	}
	var undo []func() error
	sb, err := sandbox.New(filepath.Join(o.cfg.WorkDir, s.Name))
	if err != nil {
		deleteRecord(o.cfg.StateDir, s.Name)
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
		deleteRecord(o.cfg.StateDir, s.Name)
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
			if err := o.persistLocked(st); err != nil {
				return nil, err
			}
			continue
		}
		if err := o.startServiceLocked(st, svc, rt, &undo); err != nil {
			if errors.Is(err, errProcStart) {
				rt.failed = true
				st.env.Services[name].Failed = true
				depFailed[name] = true
				o.ledger.Append(Event{Kind: "svc-failed", Env: s.Name, Service: name,
					Detail: err.Error()})
				if perr := o.persistLocked(st); perr != nil {
					return nil, perr
				}
				continue
			}
			// 资源类失败：回滚全部已分配资源。
			if rbErr := o.runUndo(s.Name, undo); rbErr != nil {
				// 回滚自身失败：保留环境（保持 creating/active 记录）用于对账重试。
				st.cleanupErr = rbErr
				st.env.Status = StatusFailed
				if perr := o.persistLocked(st); perr != nil {
					return nil, perr
				}
				o.envs[s.Name] = st
				return st, rbErr
			}
			deleteRecord(o.cfg.StateDir, s.Name)
			return nil, err
		}
		st.env.Services[name].Running = true
	}
	st.recomputeStatusLocked()
	st.rec = o.recordLocked(st)
	st.rec.Phase = phaseActive
	if err := saveRecord(o.cfg.StateDir, st.rec); err != nil {
		return nil, err
	}
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
			o.stopServiceLocked(st, name, " (update)")
			delete(st.svcs, name)
			delete(st.env.Services, name)
			o.ledger.Append(Event{Kind: "svc-remove", Env: envName, Service: name})
			if err := o.persistLocked(st); err != nil {
				return err
			}
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
			rt.failed = false
			st.env.Services[name].Blocked = true
			st.env.Services[name].Failed = false
			depFailed[name] = true
			o.ledger.Append(Event{Kind: "svc-blocked", Env: envName, Service: name,
				Detail: "upstream failed"})
			if err := o.persistLocked(st); err != nil {
				return err
			}
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
			st.recomputeStatusLocked()
			o.persistLocked(st)
			if !errors.Is(err, errProcStart) {
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
	if err := o.persistLocked(st); err != nil {
		return err
	}
	o.ledger.Append(Event{Kind: "update", Env: envName, Detail: "status=" + string(st.env.Status)})
	return nil
}

// envCountLocked 计算全局环境数配额（active + creating；destroying 不再占额）。
func (o *Orchestrator) envCountLocked() int {
	n := 0
	for _, st := range o.envs {
		if !st.destroying {
			n++
		}
	}
	return n
}

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

	// 排队等待在获取跨进程独占锁时自然实现：持锁实例完成后下一个实例进入。
	for {
		var result *Environment
		var retErr error
		o.mu.Lock()
		lockErr := o.lock.lock(true)
		if lockErr != nil {
			o.mu.Unlock()
			return nil, lockErr
		}
		if err := o.reloadLocked(); err != nil {
			o.mu.Unlock()
			o.lock.unlock()
			return nil, err
		}
		if st, ok := o.envs[s.Name]; ok {
			if st.destroying {
				retErr = fmt.Errorf("%w: %s", ErrDestroying, s.Name)
			} else if st.specKey == key {
				result = cloneEnv(st.env)
			} else {
				retErr = o.updateLocked(st, s, key)
				result = cloneEnv(st.env)
			}
		} else if o.envCountLocked() >= o.cfg.MaxEnvs {
			retErr = fmt.Errorf("%w: max environments %d reached", ErrCapacity, o.cfg.MaxEnvs)
		} else {
			st, err := o.createLocked(s, key)
			if err != nil && st == nil {
				retErr = err
			} else {
				result = cloneEnv(st.env)
				retErr = err // 回滚失败时环境保留为 Failed
			}
		}
		o.mu.Unlock()
		o.lock.unlock()
		if retErr == nil || !errors.Is(retErr, ErrCapacity) || !o.cfg.Queue {
			return result, retErr
		}
		// 队列策略下仅对"环境数上限"等待；端口耗尽等资源错误立即返回。
		if !strings.Contains(retErr.Error(), "max environments") {
			return result, retErr
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: %v", ErrCapacity, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// Get 返回环境实例；正在销毁的环境对访问方不可见。
func (o *Orchestrator) Get(name string) (*Environment, error) {
	var env *Environment
	var err error
	if lerr := o.withLock(false, true, func() error {
		st, ok := o.envs[name]
		if !ok {
			err = fmt.Errorf("%w: %s", ErrNotFound, name)
			return nil
		}
		if st.destroying {
			err = fmt.Errorf("%w: %s", ErrDestroying, name)
			return nil
		}
		env = cloneEnv(st.env)
		return nil
	}); lerr != nil {
		return nil, lerr
	}
	return env, err
}

// teardown 停止进程、释放端口、清理沙箱；每步幂等，可安全跨重试/跨重启。
func (o *Orchestrator) teardown(st *envState) error {
	var errs []error
	names := make([]string, 0, len(st.svcs))
	for name := range st.svcs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		o.stopServiceLocked(st, name, "")
	}
	if o.cleanupHook != nil {
		if err := o.cleanupHook(st.env.Name); err != nil {
			errs = append(errs, err)
		}
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

// finishDestroyLocked 删除落盘记录，销毁真正生效一次。
func (o *Orchestrator) finishDestroyLocked(st *envState) error {
	if err := deleteRecord(o.cfg.StateDir, st.env.Name); err != nil {
		return err
	}
	delete(o.envs, st.env.Name)
	st.env.Status = StatusDestroyed
	o.ledger.Append(Event{Kind: "destroy", Env: st.env.Name})
	return nil
}

// Destroy 回收环境，幂等且并发/跨实例安全：实际销毁只生效一次。
func (o *Orchestrator) Destroy(name string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.lock.lock(true); err != nil {
		return err
	}
	defer o.lock.unlock()
	if err := o.reloadLocked(); err != nil {
		return err
	}
	st, ok := o.envs[name]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	if st.destroying {
		// 另一个调用者/实例正在回收；回收幂等，直接返回。
		return nil
	}
	st.destroying = true
	st.env.Status = StatusDestroying
	// destroying 阶段只在记录中标记：若后续 teardown 失败，则回退为 active 并
	// 保留 cleanupErr（环境仍可观测、可重试）；只有进程在此刻崩溃，
	// 重启恢复才会把 phase=destroying 的记录当作半截销毁继续做完。
	phaseRecord := o.recordLocked(st)
	phaseRecord.Phase = phaseDestroying
	_ = saveRecord(o.cfg.StateDir, phaseRecord)
	err := o.teardown(st)
	if err != nil {
		// 回收失败：回退为 active 并保留 cleanupErr，环境仍可观测、可对账重试。
		st.destroying = false
		st.cleanupErr = err
		st.env.Status = StatusFailed
		o.persistLocked(st)
		return fmt.Errorf("%w: %v", ErrRollbackFailed, err)
	}
	if err := o.finishDestroyLocked(st); err != nil {
		return err
	}
	return nil
}

// Reconcile 对账：重试失败的清理、修复外部漂移（含重启接管发现的死进程），
// 或明确报告不可收敛。
func (o *Orchestrator) Reconcile(name string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.lock.lock(true); err != nil {
		return err
	}
	defer o.lock.unlock()
	if err := o.reloadLocked(); err != nil {
		return err
	}
	st, ok := o.envs[name]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	// 1. 重试此前失败的回滚/回收，不重复释放已回收资源（各步骤幂等）。
	if st.cleanupErr != nil {
		if err := o.teardown(st); err != nil {
			st.cleanupErr = err
			st.env.Status = StatusFailed
			o.persistLocked(st)
			return fmt.Errorf("%w: %v", ErrRollbackFailed, err)
		}
		st.cleanupErr = nil
		if err := o.finishDestroyLocked(st); err != nil {
			return err
		}
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
		if rt.proc == nil || !rt.proc.Alive() {
			drift = append(drift, "proc-dead:"+svcName)
			continue
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
	// 3. 修复：重启死亡进程；端口丢失则重新分配。密钥坚持原绑定版本，失效则不可收敛。
	svcByName := map[string]*spec.ServiceSpec{}
	for i := range st.spec.Services {
		svcByName[st.spec.Services[i].Name] = &st.spec.Services[i]
	}
	restarted := map[string]bool{}
	var notConvergent []error
	for _, d := range drift {
		parts := strings.SplitN(d, ":", 3)
		svcName := parts[1]
		rt := st.svcs[svcName]
		svc := svcByName[svcName]
		switch parts[0] {
		case "proc-dead":
			// 停旧句柄并释放其端口（幂等），然后用原绑定重启。
			o.stopServiceLocked(st, svcName, " (drift)")
			nrt := newSvcRuntime(svc)
			// 保留原密钥绑定：轮换/吊销都不允许悄悄换版本。
			for envName, b := range rt.secrets {
				nrt.secrets[envName] = b
			}
			st.svcs[svcName] = nrt
			st.setSvcView(svcName, nrt)
			var undo []func() error
			if err := o.startServiceLocked(st, svc, nrt, &undo); err != nil {
				o.runUndo(name, undo)
				nrt.failed = true
				st.env.Services[svcName].Failed = true
				st.recomputeStatusLocked()
				o.persistLocked(st)
				if errors.Is(err, errProcStart) {
					o.ledger.Append(Event{Kind: "svc-failed", Env: name, Service: svcName, Detail: err.Error()})
					continue
				}
				// 密钥绑定失效（被吊销/引用消失）或资源无法满足：明确不可收敛。
				notConvergent = append(notConvergent, fmt.Errorf("restart %s: %w", svcName, err))
				continue
			}
			st.env.Services[svcName].Running = true
			restarted[svcName] = true
			o.ledger.Append(Event{Kind: "reconcile-fix", Env: name, Service: svcName, Detail: "restarted"})
		case "port-lost":
			if restarted[svcName] {
				continue // 重启已重新分配全部端口
			}
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
				notConvergent = append(notConvergent, fmt.Errorf("realloc port %s/%s: %w", svcName, pn, err))
				continue
			}
			if err := o.savePorts(); err != nil {
				notConvergent = append(notConvergent, err)
				continue
			}
			rt.ports[pn] = p
			o.ledger.Append(Event{Kind: "reconcile-fix", Env: name, Service: svcName,
				Detail: fmt.Sprintf("realloc %s=%d", pn, p)})
		}
	}
	st.recomputeStatusLocked()
	if err := o.persistLocked(st); err != nil {
		return err
	}
	if len(notConvergent) > 0 {
		return fmt.Errorf("%w: %w", ErrNotConvergent, errors.Join(notConvergent...))
	}
	return nil
}

// ReapExpired 回收到期环境；失败保留待下次重试，不误删未到期环境。
// 整轮在一个跨进程临界区内完成，因此跨实例/跨重启的回收也只真正生效一次。
func (o *Orchestrator) ReapExpired(now time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.lock.lock(true); err != nil {
		return
	}
	defer o.lock.unlock()
	if err := o.reloadLocked(); err != nil {
		return
	}
	var expired []string
	for name, st := range o.envs {
		if st.destroying || st.env.ExpiresAt.IsZero() {
			continue
		}
		if !now.Before(st.env.ExpiresAt) {
			expired = append(expired, name)
		}
	}
	sort.Strings(expired)
	for _, name := range expired {
		st := o.envs[name]
		st.destroying = true
		st.env.Status = StatusDestroying
		o.persistLocked(st)
		if err := o.teardown(st); err != nil {
			st.destroying = false
			st.cleanupErr = err
			st.env.Status = StatusFailed
			o.persistLocked(st)
			o.ledger.Append(Event{Kind: "reap-failed", Env: name, Detail: err.Error()})
			continue
		}
		o.finishDestroyLocked(st)
		o.ledger.Append(Event{Kind: "reap", Env: name})
	}
}

// List 返回全部环境名（不含已销毁）。
func (o *Orchestrator) List() []string {
	var names []string
	_ = o.withLock(false, true, func() error {
		names = make([]string, 0, len(o.envs))
		for n, st := range o.envs {
			if !st.destroying {
				names = append(names, n)
			}
		}
		sort.Strings(names)
		return nil
	})
	return names
}

// Close 回收全部环境（与历史行为一致）。
func (o *Orchestrator) Close() {
	for _, name := range o.List() {
		o.Destroy(name)
	}
}

// Shutdown 不销毁任何环境，只释放本实例持有的操作系统句柄；
// 用于模拟"正常退出但状态保留"，之后由新实例 New 接管。
func (o *Orchestrator) Shutdown() {
	o.mu.Lock()
	for _, st := range o.envs {
		// 解除与子进程的 wait 句柄绑定：进程继续运行，等待新实例接管。
		for _, rt := range st.svcs {
			if rt.proc != nil {
				rt.pid = rt.proc.PID()
				rt.proc = nil
			}
		}
	}
	o.mu.Unlock()
	o.lock.close()
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

// SecretBindings 返回某服务的环境变量名 -> 绑定密钥版本（不含密钥值）。
func (o *Orchestrator) SecretBindings(envName, svcName string) (map[string]int, error) {
	out := map[string]int{}
	err := o.withLock(false, true, func() error {
		st, ok := o.envs[envName]
		if !ok {
			return fmt.Errorf("%w: %s", ErrNotFound, envName)
		}
		rt, ok := st.svcs[svcName]
		if !ok {
			return fmt.Errorf("%w: service %s", ErrNotFound, svcName)
		}
		for env, b := range rt.secrets {
			out[env] = b.version
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
