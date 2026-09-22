// Package orchestrator 把声明式环境描述收敛为可运行的本地预览环境实例。
package orchestrator

import (
	"context"
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
	StatusBlocked    Status = "Blocked"  // 依赖失败导致阻塞
	StatusFailed     Status = "Failed"
	StatusDestroying Status = "Destroying"
	StatusDestroyed  Status = "Destroyed"
)

// ServiceState 是单个服务的运行态。
type ServiceState struct {
	Name    string
	Running bool
	Blocked bool   // 上游失败导致阻塞
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
	MaxEnvs    int           // 单机同时存在的环境数上限
	PortMin    int           // 可用端口范围
	PortMax    int
	MaxService int           // 单环境服务数配额
	WorkDir    string        // 沙箱根目录
	Queue      bool          // 达到上限时排队等待（true）还是拒绝（false）
	ReapPeriod time.Duration // 到期回收扫描周期
}

// Orchestrator 管理环境生命周期。
type Orchestrator struct {
	cfg     Config
	secrets *secrets.Store
	ports   *ports.Allocator
	ledger  *Ledger

	mu   sync.Mutex
	envs map[string]*envState
}

// envState 是内部可变状态。
type envState struct {
	env     *Environment
	spec    *spec.EnvSpec
	sandbox *sandbox.Sandbox
}

func New(cfg Config, sec *secrets.Store) *Orchestrator {
	return &Orchestrator{
		cfg:     cfg,
		secrets: sec,
		ports:   ports.New(cfg.PortMin, cfg.PortMax),
		ledger:  NewLedger(),
		envs:    map[string]*envState{},
	}
}

// Ledger 返回资源流水（测试打印判定依据用）。
func (o *Orchestrator) Ledger() *Ledger { return o.ledger }

// Apply 幂等地创建或更新环境：同一描述重复提交得到同一实例。
func (o *Orchestrator) Apply(ctx context.Context, s *spec.EnvSpec) (*Environment, error) {
	return nil, nil
}

// Get 返回环境实例；正在销毁的环境对访问方不可见。
func (o *Orchestrator) Get(name string) (*Environment, error) { return nil, nil }

// Destroy 回收环境，幂等且并发安全。
func (o *Orchestrator) Destroy(name string) error { return nil }

// Reconcile 对账：修复偏离或明确报告不可收敛。
func (o *Orchestrator) Reconcile(name string) error { return nil }

// ReapExpired 回收到期环境；失败可重试，不得误删仍在使用的环境。
func (o *Orchestrator) ReapExpired(now time.Time) {}

// Close 回收全部环境。
func (o *Orchestrator) Close() {}
