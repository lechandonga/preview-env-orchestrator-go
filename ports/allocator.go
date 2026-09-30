// Package ports 提供有界、可配置上限的独占端口分配器。
package ports

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

var (
	ErrExhausted = errors.New("port pool exhausted")
	ErrConflict  = errors.New("preferred port in use")
)

// Allocator 在 [min,max] 范围内分配独占端口。
type Allocator struct {
	mu       sync.Mutex
	min, max int
	used     map[int]string // port -> owner
}

func New(min, max int) *Allocator {
	return &Allocator{min: min, max: max, used: map[int]string{}}
}

// Snapshot 返回当前持有者映射的拷贝（持久化用）。
func (a *Allocator) Snapshot() map[int]string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[int]string, len(a.used))
	for p, o := range a.used {
		out[p] = o
	}
	return out
}

// Restore 用持有者映射重建内存状态（重启/跨实例加载用）。
func (a *Allocator) Restore(used map[int]string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.used = map[int]string{}
	for p, o := range used {
		if p < a.min || p > a.max {
			continue
		}
		a.used[p] = o
	}
}

// Save 把当前分配原子写入 path（调用方必须已持有跨进程锁）。
func (a *Allocator) Save(path string) error {
	data, err := json.Marshal(a.Snapshot())
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Load 从 path 恢复分配；文件不存在视为空状态。
func (a *Allocator) Load(path string) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var used map[int]string
	if err := json.Unmarshal(data, &used); err != nil {
		return err
	}
	a.Restore(used)
	return nil
}

// EnsureFile 在文件不存在时写入初始空状态（首次初始化用）。
func (a *Allocator) EnsureFile(path string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return a.Save(path)
}

// Allocate 为 owner 分配端口。
// 策略：preferred 在范围内且空闲则使用；被占用则重新分配最小空闲端口；
// 池耗尽返回 ErrExhausted，绝不无界占用。
func (a *Allocator) Allocate(owner string, preferred int) (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if preferred >= a.min && preferred <= a.max {
		if _, taken := a.used[preferred]; !taken {
			a.used[preferred] = owner
			return preferred, nil
		}
		// 冲突：按既定策略重新分配（落到下面的最小空闲端口）。
	}
	for p := a.min; p <= a.max; p++ {
		if _, taken := a.used[p]; !taken {
			a.used[p] = owner
			return p, nil
		}
	}
	return 0, ErrExhausted
}

// Release 释放单个端口；只有持有者能释放，幂等。
// 返回 true 表示本次调用真正释放了端口（用于保证跨重启只释放一次）。
func (a *Allocator) Release(owner string, port int) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.used[port] == owner {
		delete(a.used, port)
		return true
	}
	return false
}

// ReleaseAll 释放 owner 的全部端口，返回释放列表（用于对账与日志）。
func (a *Allocator) ReleaseAll(owner string) []int {
	a.mu.Lock()
	defer a.mu.Unlock()
	var freed []int
	for p, o := range a.used {
		if o == owner {
			freed = append(freed, p)
		}
	}
	sort.Ints(freed)
	for _, p := range freed {
		delete(a.used, p)
	}
	return freed
}

// Owner 返回端口持有者，供对账校验。
func (a *Allocator) Owner(port int) (string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	o, ok := a.used[port]
	return o, ok
}

// InUse 返回当前占用数。
func (a *Allocator) InUse() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.used)
}
