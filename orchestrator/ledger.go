package orchestrator

// Event 记录一次资源分配/释放流水。
type Event struct {
	Kind    string // alloc-port, release-port, start-proc, stop-proc, rollback, ...
	Env     string
	Service string
	Detail  string
}

// Ledger 是线程安全的流水日志，供测试与审计打印判定依据。
type Ledger struct {
	events []Event
}

func NewLedger() *Ledger { return &Ledger{} }

func (l *Ledger) Append(e Event) {}

// Events 返回流水快照。
func (l *Ledger) Events() []Event { return nil }
