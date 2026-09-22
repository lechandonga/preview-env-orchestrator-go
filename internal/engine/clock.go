package engine

import "time"

// clockNow 集中时间来源，便于测试控制过期时刻。
func (e *Engine) clockNow() time.Time { return time.Now() }
