package engine

import (
	"context"
	"sync"
	"time"

	"fmt"
	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/resources"
	"github.com/lechandonga/preview-env-orchestrator-go/internal/sandbox"
	"github.com/lechandonga/preview-env-orchestrator-go/internal/secret"
	"github.com/lechandonga/preview-env-orchestrator-go/internal/store"
)

var _ = store.LockDir

// LimitPolicy 配额达到上限时的策略。
type LimitPolicy string

const (
	PolicyReject LimitPolicy = "Reject" // 立即拒绝
	PolicyQueue  LimitPolicy = "Queue"  // 排队等待名额释放（按等待顺序唤醒）
)

// Config 是引擎配置。
type Config struct {
	StateDir        string
	MaxEnvironments int
	PortMin         int
	PortMax         int
	LimitPolicy     LimitPolicy
	SecretStore     *secret.Store
}

// envLock 串行化“同一个环境”上的全部调谐/销毁操作。
// 同时持有进程内互斥（同进程 goroutine）与基于 flock 的文件锁
// （跨引擎实例/跨进程），锁在崩溃时由内核自动释放，不留死锁。
type envLock struct {
	mu sync.Mutex
}

// lock/unlock 封装两层锁定。调用方必须配对。
func (e *Engine) lockEnvFully(name string) (*store.FileLock, func()) {
	l := e.lockEnv(name)
	l.mu.Lock()
	fl, err := store.LockDir(e.cfg.StateDir+"/", "env-"+name)
	if err != nil {
		l.mu.Unlock()
		panic(fmt.Sprintf("acquire file lock for env %q: %v", name, err))
	}
	return fl, func() {
		_ = fl.Unlock()
		l.mu.Unlock()
	}
}

// Engine 是环境编排器：同一环境的并发调谐被串行化，不同环境可并行推进。
type Engine struct {
	cfg     Config
	store   *store.Store
	ports   *resources.PortPool
	quota   *resources.Quota
	sandbox *sandbox.Manager
	secrets *secret.Store
	logf    func(string, ...any)

	locksMu sync.Mutex
	locks   map[string]*envLock

	// 配额等待队列（PolicyQueue）：head 被关闭时队首等待者被唤醒。
	queueMu sync.Mutex
	waiters []*queueSlot

	// 同环境并发创建去重。
	inflightMu sync.Mutex
	inflight   map[string]*inflightGate
}

// queueSlot 表示一个排队等待配额名额的请求。
type queueSlot struct {
	ready chan struct{} // 名额可用或 ctx 取消仲裁后关闭
}

// New 创建引擎。
func New(cfg Config) (*Engine, error) {
	if cfg.StateDir == "" {
		cfg.StateDir = ".preview-orchestrator"
	}
	if cfg.PortMin <= 0 {
		cfg.PortMin = 20000
	}
	if cfg.PortMax <= 0 {
		cfg.PortMax = 20100
	}
	if cfg.PortMax < cfg.PortMin {
		return nil, &BadConfigError{Reason: "PortMax < PortMin"}
	}
	if cfg.LimitPolicy == "" {
		cfg.LimitPolicy = PolicyReject
	}
	if cfg.SecretStore == nil {
		cfg.SecretStore = secret.NewStore()
	}

	st, err := store.NewStore(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	return &Engine{
		cfg:      cfg,
		store:    st,
		ports:    resources.NewPortPool(cfg.PortMin, cfg.PortMax),
		quota:    resources.NewQuota(cfg.MaxEnvironments),
		sandbox:  sandbox.NewManager(cfg.StateDir),
		secrets:  cfg.SecretStore,
		logf:     func(string, ...any) {},
		locks:    map[string]*envLock{},
		inflight: map[string]*inflightGate{},
	}, nil
}

// NewAndRecover 创建引擎并依据历史快照恢复资源簿记（崩溃/重启后入口）。
func NewAndRecover(cfg Config) (*Engine, error) {
	e, err := New(cfg)
	if err != nil {
		return nil, err
	}
	if err := e.recoverBookkeeping(); err != nil {
		return nil, err
	}
	return e, nil
}

// SetLogger 注入判定依据/资源流水日志函数（测试中打印详细流水）。
func (e *Engine) SetLogger(f func(string, ...any)) {
	if f == nil {
		f = func(string, ...any) {}
	}
	e.logf = f
}

// lockEnv 获取某环境的专用互斥锁（不同环境互不阻塞）。
func (e *Engine) lockEnv(name string) *envLock {
	e.locksMu.Lock()
	defer e.locksMu.Unlock()
	l, ok := e.locks[name]
	if !ok {
		l = &envLock{}
		e.locks[name] = l
	}
	return l
}

// acquireQuota 按配置策略申请一个环境名额。
// Reject：名额满立即返回 QuotaExceededError；
// Queue：按 FIFO 在有空闲名额时返回，ctx 取消则放弃排队。
func (e *Engine) acquireQuota(ctx context.Context, resource string) error {
	if e.quota.TryAcquire() {
		return nil
	}
	if e.cfg.LimitPolicy != PolicyQueue {
		return &domain.QuotaExceededError{
			Resource: "environments",
			Reason:   "max concurrent environments reached",
		}
	}

	slot := &queueSlot{ready: make(chan struct{})}
	e.queueMu.Lock()
	e.waiters = append(e.waiters, slot)
	e.queueMu.Unlock()
	e.logf("quota: env %s queued (position=%d, max=%d)", resource, len(e.waiters), e.cfg.MaxEnvironments)

	select {
	case <-slot.ready:
		// 被 release 路径唤醒；名额已由唤醒方代为获取。
		return nil
	case <-ctx.Done():
		e.removeWaiter(slot)
		return ctx.Err()
	}
}

// releaseQuota 归还名额，并把名额转交给队首等待者（若有），否则直接归还。
func (e *Engine) releaseQuota() error {
	e.queueMu.Lock()
	if len(e.waiters) > 0 {
		head := e.waiters[0]
		e.waiters = e.waiters[1:]
		e.queueMu.Unlock()
		close(head.ready) // 名额“转移”给等待者，因此不调用 quota.Release。
		return nil
	}
	e.queueMu.Unlock()
	return e.quota.Release()
}

func (e *Engine) removeWaiter(slot *queueSlot) {
	e.queueMu.Lock()
	defer e.queueMu.Unlock()
	for i, w := range e.waiters {
		if w == slot {
			e.waiters = append(e.waiters[:i], e.waiters[i+1:]...)
			return
		}
	}
}

// 占位：避免未使用导入告警（时间/上下文在后续文件中使用）。
var _ = time.Time{}
var _ = context.Background

// PortsInUse 返回端口池当前登记数（主要用于测试/可观测性核对）。
func (e *Engine) PortsInUse() int { return e.ports.InUse() }

// QuotaInUse 返回配额当前占用数。
func (e *Engine) QuotaInUse() int { return e.quota.InUse() }

// inflightGate 协调同一环境名的并发创建：只有一个“领队”真正去申请名额并执行
// 创建，其余“跟随者”等待领队完成后复用结果，避免并发创建各自抢占名额导致
// 配额被无关请求挤爆（同一环境只应消耗一个名额）。
type inflightGate struct {
	done chan struct{}
	env  *domain.Environment
	err  error
}

// beginCreate 尝试成为 name 的创建领队；leader=true 时调用方负责 endCreate。
// leader=false 时 wait() 等待领队结果。
func (e *Engine) beginCreate(name string) (gate *inflightGate, leader bool) {
	e.inflightMu.Lock()
	defer e.inflightMu.Unlock()
	if g, ok := e.inflight[name]; ok {
		return g, false
	}
	g := &inflightGate{done: make(chan struct{})}
	e.inflight[name] = g
	return g, true
}

// endCreate 记录领队结果并唤醒全部跟随者。
func (e *Engine) endCreate(name string, g *inflightGate, env *domain.Environment, err error) {
	g.env, g.err = env, err
	close(g.done)
	e.inflightMu.Lock()
	// 仅当 map 中仍是自己这个门禁时才摘除，避免误删后来的新一轮。
	if cur, ok := e.inflight[name]; ok && cur == g {
		delete(e.inflight, name)
	}
	e.inflightMu.Unlock()
}

// SetFaultRemoveEnvDir 注入沙箱目录删除故障（测试专用）。
func (e *Engine) SetFaultRemoveEnvDir(err error) { e.sandbox.SetFailRemoveEnvDir(err) }
