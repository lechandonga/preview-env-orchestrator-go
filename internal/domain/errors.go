package domain

import "strings"

// ValidationError 描述非法的环境声明（字段缺失、重名、未知依赖等）。
type ValidationError struct{ Reasons []string }

func (e *ValidationError) Error() string {
	return "validation failed: " + strings.Join(e.Reasons, "; ")
}

// ConflictError 描述提交与已存在环境不可调和的冲突（如不可变字段不同）。
type ConflictError struct {
	Env    string
	Reason string
}

func (e *ConflictError) Error() string { return "environment conflict for " + e.Env + ": " + e.Reason }

// CycleError 描述服务依赖图中的环。
type CycleError struct{ Cycle []string }

func (e *CycleError) Error() string {
	return "dependency cycle detected: " + strings.Join(e.Cycle, " -> ")
}

// SecretError 描述密钥引用解析失败（缺失/版本失效）。
type SecretError struct {
	Ref    string
	Reason string
}

func (e *SecretError) Error() string { return "secret " + e.Ref + ": " + e.Reason }

// QuotaExceededError 表示环境数或端口等资源配额已满。
type QuotaExceededError struct {
	Resource string
	Reason   string
}

func (e *QuotaExceededError) Error() string {
	return "quota exceeded for " + e.Resource + ": " + e.Reason
}

// RollbackError 表示正向步骤失败后，回滚已分配资源时又发生失败（可区分、可重试）。
type RollbackError struct {
	Phase string
	Errs  []error
}

func (e *RollbackError) Error() string {
	msgs := make([]string, len(e.Errs))
	for i, err := range e.Errs {
		msgs[i] = err.Error()
	}
	return "rollback failed at " + e.Phase + ": " + strings.Join(msgs, "; ")
}

// Unwrap 使 errors.Is/As 能穿透到内部回滚错误。
func (e *RollbackError) Unwrap() []error { return e.Errs }

// NotConvergeableError 表示对账发现实际状态无法收敛到期望状态。
type NotConvergeableError struct {
	Env    string
	Reason string
}

func (e *NotConvergeableError) Error() string {
	return "environment " + e.Env + " cannot converge: " + e.Reason
}
