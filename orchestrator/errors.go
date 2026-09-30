package orchestrator

import (
	"errors"
	"fmt"
)

// 可区分的失败原因。
var (
	ErrInvalidSpec     = errors.New("invalid spec")
	ErrConflict        = errors.New("spec conflicts with existing environment")
	ErrCapacity        = errors.New("capacity limit reached")
	ErrSecretRef       = errors.New("secret reference resolution failed")
	ErrNotFound        = errors.New("environment not found")
	ErrDestroying      = errors.New("environment is being destroyed")
	ErrRollbackFailed  = errors.New("rollback failed")
	ErrNotConvergent   = errors.New("environment cannot converge")
	ErrDependencyCycle = errors.New("dependency cycle detected")
)

// joinedErr 让一个错误同时归属于两个哨兵错误（errors.Is 两者皆为 true），
// 例如密钥吊销导致的不可收敛：ErrNotConvergent 与 ErrSecretRef 同时成立。
type joinedErr struct {
	outer error
	inner error
	msg   string
}

func (e *joinedErr) Error() string { return e.msg + ": " + e.inner.Error() }
func (e *joinedErr) Unwrap() []error {
	return []error{e.outer, e.inner}
}

func wrapBoth(outer, inner error, format string, args ...any) error {
	return &joinedErr{outer: outer, inner: inner, msg: fmt.Sprintf(format, args...)}
}
