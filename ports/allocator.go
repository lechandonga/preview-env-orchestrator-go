// Package ports 提供有界、可配置上限的独占端口分配器。
package ports

import (
	"errors"
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
func (a *Allocator) Release(owner string, port int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.used[port] == owner {
		delete(a.used, port)
	}
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
