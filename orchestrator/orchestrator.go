// Package orchestrator 把声明式环境描述收敛为可运行的本地预览环境实例。
//
// 状态模型（v2）：全部环境状态原子落盘到共享状态目录，多个引擎实例
// 通过目录内全局 flock 串行化写操作；实例用心跳 + PID/启动时间戳判活，
// 死掉实例的环境由任一存活实例在下一次加锁时自动接管。进程用 PID +
// /proc 启动时间戳重新附挂，防止 PID 复用误接管。
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
	"github.com/lechandonga/preview-env-orchestrator-go/state"
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
	MaxEnvs      int // 环境数全局上限（多实例共享状态时全局生效）
	PortMin      int // 可用端口范围
	PortMax      int
	MaxService   int           // 单环境服务数配额
	WorkDir      string        // 沙箱根目录
	StateDir     string        // 共享状态目录（默认 WorkDir/state）
	Queue        bool          // 达到上限时排队等待（true）还是拒绝（false）
	ReadyTimeout time.Duration // 进程就绪观察窗口
	Heartbeat    time.Duration // 实例心跳间隔（默认 200ms）
	InstTimeout  time.Duration // 实例心跳超时判死（默认 1s）
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
	if c.Heartbeat <= 0 {
		c.Heartbeat = 200 * time.Millisecond
	}
	if c.InstTimeout <= 0 {
		c.InstTimeout = time.Second
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
	tombstoned bool  // 已决定销毁（记录已持久化）；清理可能仍在重试
	tearing    bool  // 本进程正在执行销毁（瞬时状态：对 Get/List 不可见）
	cleanupErr error // 回滚/回收失败，待对账重试
}

// Orchestrator 管理环境生命周期。
type Orchestrator struct {
	cfg     Config
	secrets *secrets.Store
	ledger  *Ledger

	store *state.Store

	mu       sync.Mutex // 同进程串行化；跨进程靠 store 全局 flock
	envs     map[string]*envState
	ports    *ports.Allocator
	inst     *state.Instance
	stopCh   chan struct{}
	stopOnce sync.Once

	cleanupHook func(envName string) error // 测试钩子：注入清理失败
}

// New 创建引擎并打开共享状态；已有状态会被加载（恢复/接管）。
func New(cfg Config, sec *secrets.Store) (*Orchestrator, error) {
	cfg.normalize()
	if sec == nil {
		sec = secrets.NewStore()
	}
	st, err := state.Open(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	o := &Orchestrator{
		cfg:     cfg,
		secrets: sec,
		ledger:  NewLedger(),
		store:   st,
		envs:    map[string]*envState{},
		ports:   ports.New(cfg.PortMin, cfg.PortMax),
		stopCh:  make(chan struct{}),
	}
	id := state.NewInstanceID()

	unlock := st.Lock()
	if stale, err := st.ReapStale(time.Now(), cfg.InstTimeout, id); err == nil {
		for _, in := range stale {
			o.auditLocked(Event{Kind: "takeover", Detail: fmt.Sprintf("dead-instance=%s pid=%d", in.ID, in.PID)})
		}
	}
	if err := o.loadAllLocked(); err != nil {
		unlock()
		return nil, err
	}
	inst, err := st.RegisterInstance(id)
	if err != nil {
		unlock()
		return nil, err
	}
	o.inst = inst
	// 启动恢复：续做中断的墓碑清理，再收敛创建到一半的环境；
	// 仍在跑的进程只附挂、不重启。
	o.recoverLocked()
	unlock()

	go o.heartbeatLoop()
	return o, nil
}

func (o *Orchestrator) heartbeatLoop() {
	t := time.NewTicker(o.cfg.Heartbeat)
	defer t.Stop()
	for {
		select {
		case <-o.stopCh:
			return
		case <-t.C:
			o.mu.Lock()
			o.store.Heartbeat(o.inst)
			o.mu.Unlock()
		}
	}
}

// Ledger 返回资源流水（测试打印判定依据用）。
func (o *Orchestrator) Ledger() *Ledger { return o.ledger }

// Ports 返回端口分配器（测试断言用）。
func (o *Orchestrator) Ports() *ports.Allocator {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.ports
}

// gate 进入一次临界区：同进程互斥 + 跨进程全局锁，并从磁盘刷新全量状态。
func (o *Orchestrator) gate() func() {
	o.mu.Lock()
	unlock := o.store.Lock()
	o.store.Heartbeat(o.inst)
	if stale, err := o.store.ReapStale(time.Now(), o.cfg.InstTimeout, o.inst.ID); err == nil {
		for _, in := range stale {
			o.auditLocked(Event{Kind: "takeover", Detail: fmt.Sprintf("dead-instance=%s pid=%d", in.ID, in.PID)})
		}
	}
	o.reloadLocked()
	return func() {
		unlock()
		o.mu.Unlock()
	}
}

// auditLocked 同时写本进程流水与跨进程共享 journal（持锁调用）。
func (o *Orchestrator) auditLocked(e Event) {
	o.ledger.Append(e)
	_ = o.store.AppendJournal(e)
}

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

// loadAllLocked 从状态目录加载全部环境记录，附挂仍存活的进程，
// 并按记录重建端口分配表（已占用端口绝不可能被再次分配）。
func (o *Orchestrator) loadAllLocked() error {
	names, err := o.store.List("envs")
	if err != nil {
		return err
	}
	o.envs = map[string]*envState{}
	o.ports = ports.New(o.cfg.PortMin, o.cfg.PortMax)
	for _, fn := range names {
		if !strings.HasSuffix(fn, ".json") {
			continue
		}
		var rec envRecord
		ok, err := o.store.ReadJSON(filepath.Join("envs", fn), &rec)
		if err != nil || !ok || rec.SchemaV != stateSchemaV {
			continue // 半写文件由原子写保证不会出现；未知版本跳过
		}
		o.ingestRecordLocked(&rec)
	}
	return nil
}

// reloadLocked 每次进临界区都重新加载（别的实例可能已经改过状态）。
func (o *Orchestrator) reloadLocked() {
	if err := o.loadAllLocked(); err != nil {
		o.ledger.Append(Event{Kind: "reload-failed", Detail: err.Error()})
	}
}

func (o *Orchestrator) ingestRecordLocked(rec *envRecord) {
	env := &Environment{
		Name:      rec.Name,
		SpecKey:   rec.SpecKey,
		Status:    rec.Status,
		Services:  map[string]*ServiceState{},
		CreatedAt: rec.CreatedAt,
	}
	if rec.ExpiresAt != nil {
		env.ExpiresAt = *rec.ExpiresAt
	}
	sb, _ := sandbox.New(filepath.Join(o.cfg.WorkDir, rec.Name))
	st := &envState{
		env:        env,
		spec:       rec.Spec,
		specKey:    rec.SpecKey,
		sb:         sb,
		svcs:       map[string]*svcRuntime{},
		tombstoned: rec.Tombstoned,
	}
	for sn, sr := range rec.Services {
		rt := &svcRuntime{
			ports:   map[string]int{},
			secrets: map[string]boundSecret{},
			svcKey:  sr.SvcKey,
			blocked: sr.Blocked,
			failed:  sr.Failed,
		}
		for pn, p := range sr.Ports {
			rt.ports[pn] = p
			if !rec.Tombstoned {
				o.ports.Restore(rec.Name, p)
			}
		}
		for _, b := range sr.Secrets {
			rt.secrets[b.Env] = boundSecret{key: b.Key, version: b.Version}
		}
		if sr.Proc != nil {
			// 重新附挂仍在运行的进程；PID 复用由启动时间戳拦截。
			if p := sb.Attach(sn, sr.Proc.PID, sr.Proc.StartTicks); p != nil {
				rt.proc = p
			}
		}
		st.svcs[sn] = rt
		view := &ServiceState{Name: sn, Ports: map[string]int{}, Blocked: rt.blocked, Failed: rt.failed}
		for pn, p := range rt.ports {
			view.Ports[pn] = p
		}
		if rt.proc != nil {
			view.Running = true
		}
		env.Services[sn] = view
	}
	o.envs[rec.Name] = st
}

// activeCountLocked 统计占用全局槽位的环境数：墓碑环境占槽位直到清理真正完成。
func (o *Orchestrator) activeCountLocked() int {
	n := 0
	for _, st := range o.envs {
		if !st.tombstoned {
			n++
		}
	}
	return n
}

// resolveSecretLocked 解析密钥：优先使用已绑定版本（轮换/重启都不改变），
// 否则按引用解析当前最新版本并固定。吊销/缺失一律明确失败。
// 返回（环境变量赋值串, 绑定版本）。
func (o *Orchestrator) resolveSecretLocked(st *envState, rt *svcRuntime, ref spec.SecretRef) (string, int, error) {
	version := ref.Version
	if b, ok := rt.secrets[ref.Env]; ok {
		version = b.version // 已绑定：永远读当初那个版本，绝不悄悄换最新。
	} else if version == 0 {
		latest, err := o.secrets.Latest(ref.Key)
		if err != nil {
			return "", 0, err
		}
		version = latest
	}
	val, err := o.secrets.Get(ref.Key, version)
	if err != nil {
		return "", 0, err
	}
	rt.secrets[ref.Env] = boundSecret{key: ref.Key, version: version}
	return ref.Env + "=" + val, version, nil
}

// startServiceLocked 为单个服务分配端口、解析密钥、启动进程并等待就绪。
// pinned=true 表示重启/恢复：密钥绑定版本沿用既有绑定，不重新解析最新。
// 资源类失败返回包装了 ErrCapacity/ErrSecretRef 的错误；
// 进程类失败返回包装 errProcStart 的错误。
func (o *Orchestrator) startServiceLocked(st *envState, svc *spec.ServiceSpec, rt *svcRuntime, pinned bool, undo *[]func() error) error {
	envName := st.env.Name
	// 1. 端口。重启（端口已在 rt.ports 中）不重新分配；丢失的端口由对账补。
	for _, pr := range svc.Ports {
		if existing, ok := rt.ports[pr.Name]; ok {
			if _, owned := o.ports.Owner(existing); owned {
				continue
			}
			// 记录里的端口已不在分配表：恢复占用，不换端口。
			o.ports.Restore(envName, existing)
			continue
		}
		p, err := o.ports.Allocate(envName, pr.Preferred)
		if err != nil {
			return fmt.Errorf("%w: service %s port %s: %v", ErrCapacity, svc.Name, pr.Name, err)
		}
		rt.ports[pr.Name] = p
		o.persistEnvLocked(st)
		o.auditLocked(Event{Kind: "alloc-port", Env: envName, Service: svc.Name,
			Detail: fmt.Sprintf("%s=%d", pr.Name, p)})
		port, name := p, pr.Name
		*undo = append(*undo, func() error {
			o.ports.Release(envName, port)
			o.auditLocked(Event{Kind: "release-port", Env: envName, Service: svc.Name,
				Detail: fmt.Sprintf("%s=%d (rollback)", name, port)})
			return nil
		})
	}
	// 2. 密钥引用：绑定时解析并固定版本；缺失/吊销/版本失效 = 明确失败。
	envVars := []string{}
	for k, v := range svc.Env {
		envVars = append(envVars, k+"="+v)
	}
	for _, ref := range svc.SecretRefs {
		assign, version, err := o.resolveSecretLocked(st, rt, ref)
		if err != nil {
			return fmt.Errorf("%w: service %s: secret %q: %v", ErrSecretRef, svc.Name, ref.Key, err)
		}
		envVars = append(envVars, assign)
		o.persistEnvLocked(st)
		// 流水只记录引用与版本，绝不记录值。
		o.auditLocked(Event{Kind: "bind-secret", Env: envName, Service: svc.Name,
			Detail: fmt.Sprintf("%s@%d -> $%s", ref.Key, version, ref.Env)})
	}
	// 3. 端口注入为环境变量（PORT_<NAME>）。
	for name, p := range rt.ports {
		envVars = append(envVars, "PORT_"+strings.ToUpper(name)+"="+fmt.Sprint(p))
	}
	// 4. 启动进程。
	proc, err := st.sb.Start(svc.Name, svc.Command, envVars)
	if err != nil {
		return fmt.Errorf("%w: service %s: %v", errProcStart, svc.Name, err)
	}
	rt.proc = proc
	o.persistEnvLocked(st) // 进程 PID/启动时间戳立即落盘，强杀也能接管
	o.auditLocked(Event{Kind: "start-proc", Env: envName, Service: svc.Name,
		Detail: fmt.Sprintf("pid=%d", proc.PID())})
	*undo = append(*undo, func() error {
		proc.Stop()
		o.auditLocked(Event{Kind: "stop-proc", Env: envName, Service: svc.Name,
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
	_ = pinned
	return nil
}

// stopServiceLocked 停止服务并释放其端口，幂等（无进程/无端口时零副作用）。
func (o *Orchestrator) stopServiceLocked(st *envState, name string) {
	rt := st.svcs[name]
	if rt == nil {
		return
	}
	if rt.proc != nil {
		pid := rt.proc.PID()
		rt.proc.Stop()
		o.auditLocked(Event{Kind: "stop-proc", Env: st.env.Name, Service: name,
			Detail: fmt.Sprintf("pid=%d", pid)})
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
		o.auditLocked(Event{Kind: "release-port", Env: st.env.Name, Service: name,
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
			o.auditLocked(Event{Kind: "rollback-failed", Env: envName, Detail: err.Error()})
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
		o.auditLocked(Event{Kind: "cleanup-sandbox", Env: s.Name, Detail: "(rollback)"})
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
	// Starting 记录先落盘：创建中被强杀，恢复方看到 Starting 会收敛或清理，
	// 绝不会把已占端口/槽位再分出去，也不会留下无主孤儿。
	o.envs[s.Name] = st
	o.persistEnvLocked(st)
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
			o.persistEnvLocked(st)
			o.auditLocked(Event{Kind: "svc-blocked", Env: s.Name, Service: name,
				Detail: "upstream failed"})
			continue
		}
		if err := o.startServiceLocked(st, svc, rt, false, &undo); err != nil {
			if errors.Is(err, errProcStart) {
				// 进程失败：服务标记失败，下游阻塞，环境保留可观测。
				rt.failed = true
				st.env.Services[name].Failed = true
				depFailed[name] = true
				o.persistEnvLocked(st)
				o.auditLocked(Event{Kind: "svc-failed", Env: s.Name, Service: name,
					Detail: err.Error()})
				continue
			}
			// 资源类失败：回滚全部已分配资源。
			if rbErr := o.runUndo(s.Name, undo); rbErr != nil {
				// 回滚自身失败：保留环境用于对账重试，错误可区分。
				st.cleanupErr = rbErr
				st.env.Status = StatusFailed
				o.persistEnvLocked(st)
				return st, rbErr
			}
			o.removeEnvLocked(s.Name)
			delete(o.envs, s.Name)
			return nil, err
		}
		st.env.Services[name].Running = true
	}
	st.recomputeStatusLocked()
	o.persistEnvLocked(st)
	o.auditLocked(Event{Kind: "create", Env: s.Name, Detail: "status=" + string(st.env.Status)})
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
			o.persistEnvLocked(st)
			o.auditLocked(Event{Kind: "svc-remove", Env: envName, Service: name})
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
			o.persistEnvLocked(st)
			o.auditLocked(Event{Kind: "svc-blocked", Env: envName, Service: name,
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
		if err := o.startServiceLocked(st, svc, rt, false, &undo); err != nil {
			o.runUndo(envName, undo) // 尽力回滚本次变更
			rt.failed = true
			st.env.Services[name].Failed = true
			depFailed[name] = true
			o.persistEnvLocked(st)
			o.auditLocked(Event{Kind: "svc-failed", Env: envName, Service: name,
				Detail: err.Error()})
			if !errors.Is(err, errProcStart) {
				st.recomputeStatusLocked()
				o.persistEnvLocked(st)
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
	o.persistEnvLocked(st)
	o.auditLocked(Event{Kind: "update", Env: envName, Detail: "status=" + string(st.env.Status)})
	return nil
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

	// 容量判定在全局锁下进行（跨实例一致）；Queue 策略下轮询等待，可被 ctx 取消。
	for {
		done := o.gate()
		if st, ok := o.envs[s.Name]; ok {
			if st.tombstoned {
				done()
				return nil, fmt.Errorf("%w: %s", ErrDestroying, s.Name)
			}
			if st.specKey == key {
				env := cloneEnv(st.env)
				done()
				return env, nil // 幂等：同一描述得到同一实例
			}
			err := o.updateLocked(st, s, key)
			env := cloneEnv(st.env)
			done()
			return env, err
		}
		full := o.activeCountLocked() >= o.cfg.MaxEnvs
		if !full {
			st, err := o.createLocked(s, key)
			if err != nil && st == nil {
				done()
				return nil, err
			}
			var env *Environment
			if st != nil {
				env = cloneEnv(st.env)
			}
			done()
			return env, err
		}
		done()
		if !o.cfg.Queue {
			return nil, fmt.Errorf("%w: max environments %d reached", ErrCapacity, o.cfg.MaxEnvs)
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: %v", ErrCapacity, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// Get 返回环境实例；正在销毁（墓碑）的环境对访问方不可见。
func (o *Orchestrator) Get(name string) (*Environment, error) {
	done := o.gate()
	defer done()
	st, ok := o.envs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	if st.tombstoned && (st.tearing || st.env.Status == StatusDestroying) {
		return nil, fmt.Errorf("%w: %s", ErrDestroying, name)
	}
	return cloneEnv(st.env), nil
}

// teardown 停止进程、释放端口、清理沙箱；每步幂等，可安全重试，
// 已释放的端口/已停止的进程不会被处理第二遍。
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
	if o.cleanupHook != nil {
		if err := o.cleanupHook(st.env.Name); err != nil {
			errs = append(errs, err)
		}
	}
	if st.sb != nil {
		if err := st.sb.Cleanup(); err != nil {
			errs = append(errs, err)
			o.auditLocked(Event{Kind: "cleanup-failed", Env: st.env.Name, Detail: err.Error()})
		} else {
			o.auditLocked(Event{Kind: "cleanup-sandbox", Env: st.env.Name})
		}
	}
	return errors.Join(errs...)
}

// Destroy 回收环境，幂等且跨实例/跨重启只真正生效一次：
// 先持久化墓碑（任何观察者都能看到"销毁中"，槽位/端口不再被分配），
// 再做进程/端口/沙箱清理，全部成功后删除记录并放行槽位。
func (o *Orchestrator) Destroy(name string) error {
	done := o.gate()
	st, ok := o.envs[name]
	if !ok {
		done()
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	if st.tombstoned {
		if st.tearing || st.env.Status == StatusDestroying {
			done()
			return nil
		}
		// 此前的销毁在清理阶段失败（Failed）：任何实例都可在此重试。
		if err := o.teardown(st); err != nil {
			st.cleanupErr = err
			st.env.Status = StatusFailed
			o.persistEnvLocked(st)
			done()
			return fmt.Errorf("%w: %v", ErrRollbackFailed, err)
		}
		o.removeEnvLocked(name)
		delete(o.envs, name)
		o.auditLocked(Event{Kind: "destroy", Env: name})
		done()
		return nil
	}
	st.tombstoned = true
	st.tearing = true
	st.env.Status = StatusDestroying
	o.persistEnvLocked(st)
	err := o.teardown(st)
	if err != nil {
		// 清理失败：墓碑已落盘，环境占着槽位等待 Reconcile/接管方重试。
		st.tearing = false
		st.cleanupErr = err
		st.env.Status = StatusFailed
		o.persistEnvLocked(st)
		done()
		return fmt.Errorf("%w: %v", ErrRollbackFailed, err)
	}
	o.removeEnvLocked(name)
	delete(o.envs, name)
	o.auditLocked(Event{Kind: "destroy", Env: name})
	st.tearing = false
	done()
	return nil
}

// convergeLocked 把环境向期望状态收敛（启动恢复与对账共用）：
// 外部杀死的进程按原端口、原密钥绑定重启；端口丢失则按记录恢复或重分；
// 无法修复返回 ErrNotConvergent。
func (o *Orchestrator) convergeLocked(st *envState) error {
	name := st.env.Name
	svcByName := map[string]*spec.ServiceSpec{}
	for i := range st.spec.Services {
		svcByName[st.spec.Services[i].Name] = &st.spec.Services[i]
	}
	order, err := spec.TopoOrder(st.spec)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrDependencyCycle, err)
	}
	depFailed := map[string]bool{}
	var fixErrs []error
	for _, svcName := range order {
		rt := st.svcs[svcName]
		svc := svcByName[svcName]
		blocked := false
		for _, d := range svc.DependsOn {
			if depFailed[d] {
				blocked = true
			}
		}
		if blocked {
			if rt == nil {
				rt = newSvcRuntime(svc)
				st.svcs[svcName] = rt
				st.setSvcView(svcName, rt)
			}
			rt.blocked = true
			rt.failed = false
			st.env.Services[svcName].Blocked = true
			st.env.Services[svcName].Running = false
			depFailed[svcName] = true
			continue
		}
		if rt == nil {
			rt = newSvcRuntime(svc)
			st.svcs[svcName] = rt
			st.setSvcView(svcName, rt)
		}
		rt.blocked = false
		if rt.proc != nil && rt.proc.Alive() {
			st.env.Services[svcName].Running = true
			st.env.Services[svcName].Blocked = false
			continue
		}
		// 进程缺失/死亡：按既有密钥绑定（pinned）与原端口重启。
		if rt.proc != nil {
			rt.proc = nil
			o.auditLocked(Event{Kind: "drift", Env: name, Service: svcName, Detail: "proc-dead"})
		}
		// 端口漂移：记录端口在分配表里消失（被外部释放）则恢复归属。
		for pn, p := range rt.ports {
			if owner, ok := o.ports.Owner(p); !ok || owner != name {
				if ok {
					// 端口被别人抢走（只可能状态损坏）：换一个端口修复。
					delete(rt.ports, pn)
					np, err := o.ports.Allocate(name, 0)
					if err != nil {
						fixErrs = append(fixErrs, fmt.Errorf("%w: realloc %s/%s: %v", ErrNotConvergent, svcName, pn, err))
						continue
					}
					rt.ports[pn] = np
					o.auditLocked(Event{Kind: "reconcile-fix", Env: name, Service: svcName,
						Detail: fmt.Sprintf("realloc %s=%d", pn, np)})
				} else {
					o.ports.Restore(name, p)
					o.auditLocked(Event{Kind: "reconcile-fix", Env: name, Service: svcName,
						Detail: fmt.Sprintf("restore %s=%d", pn, p)})
				}
			}
		}
		var undo []func() error
		if err := o.startServiceLocked(st, svc, rt, true, &undo); err != nil {
			o.runUndo(name, undo)
			rt.failed = true
			st.env.Services[svcName].Failed = true
			st.env.Services[svcName].Running = false
			depFailed[svcName] = true
			o.auditLocked(Event{Kind: "svc-failed", Env: name, Service: svcName, Detail: err.Error()})
			if errors.Is(err, ErrSecretRef) {
				// 绑定版本被吊销/引用失效：不可收敛，且 ErrSecretRef 仍可判定，
				// 绝不能悄悄换成最新版本。
				fixErrs = append(fixErrs, wrapBoth(ErrNotConvergent, err, "restart %s", svcName))
			} else {
				fixErrs = append(fixErrs, fmt.Errorf("%w: restart %s: %v", ErrNotConvergent, svcName, err))
			}
			continue
		}
		rt.failed = false
		st.env.Services[svcName].Failed = false
		st.env.Services[svcName].Blocked = false
		st.env.Services[svcName].Running = true
		o.auditLocked(Event{Kind: "reconcile-fix", Env: name, Service: svcName, Detail: "restarted"})
	}
	st.recomputeStatusLocked()
	o.persistEnvLocked(st)
	if len(fixErrs) > 0 {
		return errors.Join(fixErrs...)
	}
	return nil
}

// Reconcile 对账：重试失败的清理、修复外部漂移，或明确报告不可收敛。
func (o *Orchestrator) Reconcile(name string) error {
	done := o.gate()
	defer done()
	st, ok := o.envs[name]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	// 1. 重试此前失败的回滚/回收（含别的实例崩溃留下的墓碑），不重复释放。
	if st.tombstoned {
		if err := o.teardown(st); err != nil {
			st.cleanupErr = err
			o.persistEnvLocked(st)
			return fmt.Errorf("%w: %v", ErrRollbackFailed, err)
		}
		st.cleanupErr = nil
		o.removeEnvLocked(name)
		delete(o.envs, name)
		o.auditLocked(Event{Kind: "reconcile", Env: name, Detail: "cleanup retry succeeded"})
		return nil
	}
	st.cleanupErr = nil
	// 2. 检测偏离并修复。
	drift := false
	for svcName, rt := range st.svcs {
		if rt.failed || rt.blocked {
			continue
		}
		if rt.proc == nil || !rt.proc.Alive() {
			drift = true
			break
		}
		for _, p := range rt.ports {
			if owner, ok := o.ports.Owner(p); !ok || owner != name {
				drift = true
				_ = svcName
				break
			}
		}
	}
	if !drift {
		o.auditLocked(Event{Kind: "reconcile", Env: name, Detail: "converged"})
		return nil
	}
	o.auditLocked(Event{Kind: "reconcile", Env: name, Detail: "drift detected"})
	if err := o.convergeLocked(st); err != nil {
		return err
	}
	return nil
}

// recoverLocked 启动时自动收敛：完成中断的墓碑清理；收敛创建到一半的环境；
// Ready/Blocked/Failed 环境不做任何修复动作（等待显式 Reconcile/操作）。
func (o *Orchestrator) recoverLocked() {
	for name, st := range o.envs {
		if !st.tombstoned {
			continue
		}
		if err := o.teardown(st); err != nil {
			st.cleanupErr = err
			o.persistEnvLocked(st)
			o.auditLocked(Event{Kind: "recover", Env: name, Detail: "teardown retry failed: " + err.Error()})
			continue
		}
		o.removeEnvLocked(name)
		delete(o.envs, name)
		o.auditLocked(Event{Kind: "recover", Env: name, Detail: "interrupted teardown completed"})
	}
	for name, st := range o.envs {
		if st.env.Status != StatusStarting {
			continue
		}
		if err := o.convergeLocked(st); err != nil {
			o.auditLocked(Event{Kind: "recover", Env: name, Detail: "converge failed: " + err.Error()})
		} else {
			o.auditLocked(Event{Kind: "recover", Env: name, Detail: "interrupted create converged"})
		}
	}
}

// ReapExpired 回收到期环境；失败保留待下次重试，不误删未到期环境。
func (o *Orchestrator) ReapExpired(now time.Time) {
	done := o.gate()
	var expired []string
	for name, st := range o.envs {
		if st.tearing || st.env.Status == StatusDestroying {
			continue // 别的调用者正在销毁
		}
		if st.tombstoned {
			expired = append(expired, name) // 上次回收失败，继续重试
			continue
		}
		if st.env.ExpiresAt.IsZero() {
			continue
		}
		if !now.Before(st.env.ExpiresAt) {
			expired = append(expired, name)
		}
	}
	done()
	sort.Strings(expired)
	for _, name := range expired {
		if err := o.Destroy(name); err != nil {
			o.ledger.Append(Event{Kind: "reap-failed", Env: name, Detail: err.Error()})
			continue
		}
		o.ledger.Append(Event{Kind: "reap", Env: name})
	}
}

// List 返回全部环境名（不含墓碑/已销毁）。
func (o *Orchestrator) List() []string {
	done := o.gate()
	defer done()
	names := make([]string, 0, len(o.envs))
	for n, st := range o.envs {
		if !(st.tearing || st.env.Status == StatusDestroying) {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	return names
}

// Close 回收全部环境并注销本实例（沙箱进程被停止）。单实例测试使用；
// 多实例场景下一个实例退出应使用 Stop 以保留环境给其他实例接管。
func (o *Orchestrator) Close() {
	names := o.List()
	for _, name := range names {
		o.Destroy(name)
	}
	o.Stop()
}

// Stop 注销心跳并停止后台循环；不销毁任何环境，其他实例可继续接管。
func (o *Orchestrator) Stop() {
	o.stopOnce.Do(func() {
		close(o.stopCh)
		o.mu.Lock()
		o.store.Unregister(o.inst)
		o.mu.Unlock()
	})
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
	done := o.gate()
	defer done()
	st, ok := o.envs[envName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, envName)
	}
	rt, ok := st.svcs[svcName]
	if !ok {
		return nil, fmt.Errorf("%w: service %s", ErrNotFound, svcName)
	}
	out := map[string]int{}
	for env, b := range rt.secrets {
		out[env] = b.version
	}
	return out, nil
}
