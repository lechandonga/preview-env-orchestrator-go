package resources

import (
	"errors"
	"path/filepath"
	"sync"
)

// 配额相关哨兵错误。
var (
	ErrQuotaFull     = errors.New("environment quota full")
	ErrQuotaOverdraw = errors.New("quota release without acquire")
)

// Quota 限制单机可同时存在的环境数量。
//
// 当通过 BindStateDir 绑定状态目录后，名额计数以目录下的文件信号量为权威来源，
// 从而让共享同一状态目录的多个引擎实例（多进程）也遵守同一上限；
// 未绑定时退化为纯进程内计数（用于单元测试/嵌入式用法）。
type Quota struct {
	mu   sync.Mutex
	max  int
	used int
	fs   *FSQuota
}

// NewQuota 创建配额；max<=0 表示不限制。
func NewQuota(max int) *Quota { return &Quota{max: max} }

// BindStateDir 绑定跨进程共享目录并与当前内存计数对齐。
func (q *Quota) BindStateDir(dir string) error {
	fs, err := NewFSQuota(filepath.Join(dir, "quota-state"))
	if err != nil {
		return err
	}
	q.mu.Lock()
	q.fs = fs
	q.mu.Unlock()
	return nil
}

// TryAcquire 尝试占用一个环境名额，失败立即返回 false（不阻塞）。
// 排队由引擎层基于显式等待队列实现，配额本身保持简单确定。
func (q *Quota) TryAcquire() bool {
	q.mu.Lock()
	if q.fs != nil {
		fs := q.fs
		q.mu.Unlock()
		ok, err := fs.TryAcquire(q.max)
		if err != nil {
			return false
		}
		return ok
	}
	defer q.mu.Unlock()
	if q.max > 0 && q.used >= q.max {
		return false
	}
	q.used++
	return true
}

// Release 归还名额；超量归还必须返回错误。
func (q *Quota) Release() error {
	q.mu.Lock()
	if q.fs != nil {
		fs := q.fs
		q.mu.Unlock()
		return fs.Release()
	}
	defer q.mu.Unlock()
	if q.used == 0 {
		return ErrQuotaOverdraw
	}
	q.used--
	return nil
}

// InUse 返回当前占用名额。
func (q *Quota) InUse() int {
	q.mu.Lock()
	if q.fs != nil {
		fs := q.fs
		q.mu.Unlock()
		return fs.InUse()
	}
	defer q.mu.Unlock()
	return q.used
}
