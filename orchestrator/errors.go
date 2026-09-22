package orchestrator

import "errors"

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
