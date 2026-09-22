package engine

// BadConfigError 表示引擎配置非法。
type BadConfigError struct{ Reason string }

func (e *BadConfigError) Error() string { return "bad engine config: " + e.Reason }
