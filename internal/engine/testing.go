package engine

import (
	"time"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"
)

// DebugLoadSnapshot / DebugSaveSnapshot 暴露持久化快照读写，供测试模拟崩溃点
// （例如把环境标记为 Deleting 来模拟删除中途断电）与运维排障使用。
func (e *Engine) DebugLoadSnapshot(name string) (*domain.Environment, bool, error) {
	return e.store.Get(name)
}

func (e *Engine) DebugSaveSnapshot(env *domain.Environment) error { return e.store.Put(env) }

// DebugNow 返回当前时间，供测试构造对账时刻。
func DebugNow() time.Time { return time.Now() }
