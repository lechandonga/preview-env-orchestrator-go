// Package engine 把声明式环境描述调谐为本地可运行的预览环境实例。
//
// 错误类型统一定义在 internal/domain（ValidationError、CycleError、
// SecretError、ConflictError、QuotaExceededError、RollbackError、
// NotConvergeableError），以便 graph 等下层包与调用方共享而不产生导入环。
package engine
