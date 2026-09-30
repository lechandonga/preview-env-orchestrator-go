package orchestrator

import (
	"fmt"
	"sync"
	"time"
)

// Event 记录一次资源分配/释放流水。
type Event struct {
	At      time.Time `json:"at"`
	Kind    string    `json:"kind"` // alloc-port, release-port, start-proc, stop-proc, bind-secret, rollback, reconcile, reap, ...
	Env     string    `json:"env"`
	Service string    `json:"svc,omitempty"`
	Detail  string    `json:"detail,omitempty"`
}

func (e Event) String() string {
	return fmt.Sprintf("%s [%s] env=%s svc=%s %s", e.At.Format("15:04:05.000"), e.Kind, e.Env, e.Service, e.Detail)
}

// Ledger 是线程安全的流水日志，供测试与审计打印判定依据。
type Ledger struct {
	mu     sync.Mutex
	events []Event
	sink   func(Event) // 可选的持久化出口（由引擎注入，落共享流水文件）
}

func NewLedger() *Ledger { return &Ledger{} }

func (l *Ledger) Append(e Event) {
	e.At = time.Now()
	l.mu.Lock()
	l.events = append(l.events, e)
	sink := l.sink
	l.mu.Unlock()
	if sink != nil {
		sink(e)
	}
}

// setSink 设置持久化出口并预填历史流水（重启加载用）。
func (l *Ledger) setSink(history []Event, sink func(Event)) {
	l.mu.Lock()
	l.events = append([]Event(nil), history...)
	l.sink = sink
	l.mu.Unlock()
}

// Events 返回流水快照。
func (l *Ledger) Events() []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Event(nil), l.events...)
}

// Count 统计某类事件数量（测试断言用）。
func (l *Ledger) Count(kind string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, e := range l.events {
		if e.Kind == kind {
			n++
		}
	}
	return n
}
