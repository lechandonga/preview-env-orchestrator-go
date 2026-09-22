// Package ports 提供有界、可配置上限的独占端口分配器。
package ports

import "errors"

var (
	ErrExhausted = errors.New("port pool exhausted")
	ErrConflict  = errors.New("preferred port in use")
)

// Allocator 在 [min,max] 范围内分配独占端口。
type Allocator struct {
	min, max int
	used     map[int]string // port -> owner
}

func New(min, max int) *Allocator { return &Allocator{min: min, max: max, used: map[int]string{}} }

// Allocate 为 owner 分配端口；preferred 被占用时按策略重分配，池耗尽返回 ErrExhausted。
func (a *Allocator) Allocate(owner string, preferred int) (int, error) { return 0, nil }

// Release 释放单个端口。
func (a *Allocator) Release(owner string, port int) {}

// ReleaseAll 释放 owner 的全部端口，返回释放列表（用于对账与日志）。
func (a *Allocator) ReleaseAll(owner string) []int { return nil }

// InUse 返回当前占用数。
func (a *Allocator) InUse() int { return 0 }
