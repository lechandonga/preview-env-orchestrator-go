# preview-env-orchestrator-go

本地预览环境编排与生命周期管理组件。用**本地进程 + 文件系统**模拟隔离沙箱，
把声明式的环境描述（`EnvSpec`）收敛成可运行的预览环境实例，使创建、访问、
变更与回收在异常与并发场景下都有**确定且可解释**的结果。

## 环境模型

```
EnvSpec（声明式描述，不可变输入）
 ├── Name / TTL（0 = 不过期）
 └── []ServiceSpec
      ├── Name / Command（本地进程命令）
      ├── DependsOn      上游依赖，决定启动顺序
      ├── Ports          独占端口申请（Preferred=0 表示自动分配）
      ├── Env            明文环境变量
      └── SecretRefs     密钥引用（Key + Version + 注入的 Env 名）
```

- `EnvSpec.Key()` 是描述的内容指纹，作为**幂等键**：同一描述重复 `Apply`
  得到同一个环境实例，不追加任何副作用。
- 每个环境实例有状态机：`Starting → Ready / Blocked / Failed → Destroying → Destroyed`。
- 沙箱 = 独立工作目录 + 独立进程组（`Setpgid`），进程输出写入沙箱内日志文件。

## 调谐规则

1. **幂等创建**：`Apply` 先校验描述，再按名称查找已有环境——
   指纹相同直接返回现有实例；指纹不同进入**更新**流程。
2. **更新收敛**：更新按服务级指纹做 diff——移除的服务被停止并释放端口，
   变化的服务被替换，**未变化的服务原样保留**（端口、进程不动），
   重复提交相同描述零副作用。
3. **依赖编排**：服务按拓扑序启动；上游在就绪观察窗口内退出即判定失败，
   其全部下游被标记为 `Blocked`（不是静默跳过），无依赖的分支照常启动。
   依赖图有环时描述被直接拒绝（`ErrDependencyCycle`）。
4. **失败回滚**：创建过程中的资源类失败（端口耗尽、密钥缺失等）会**逆序回滚**
   已分配的全部资源（停进程、释放端口、删沙箱）；回滚自身失败返回可区分的
   `ErrRollbackFailed`，环境保留为 `Failed` 待 `Reconcile` 重试，不留孤儿资源。
5. **对账（Reconcile）**：
   - 优先重试此前失败的清理（各步骤幂等，不重复释放）；
   - 检测外部漂移（进程被杀、端口被外部释放）并修复（重启进程、重分配端口）；
   - 无法修复时返回 `ErrNotConvergent` 明确报告。
6. **到期回收**：`ReapExpired(now)` 只回收 `ExpiresAt` 已到期的环境，
   不误删未到期环境；回收失败记录 `reap-failed` 流水并保留环境，下次调用重试。

## 资源与密钥约束

- **端口**：`Allocator` 只在 `[PortMin, PortMax]` 内分配。首选端口冲突时按策略
  重分配最小空闲端口；池耗尽返回 `ErrExhausted`（包装为 `ErrCapacity`），
  绝不无界占用。只有持有者能释放端口，释放幂等。
- **配额**：`MaxEnvs`（单机环境数上限，信号量实现）、`MaxService`（单环境服务数）。
  达到上限时按 `Config.Queue` 策略**排队等待**（可被 ctx 取消）或**立即拒绝**
  （`ErrCapacity`）。
- **密钥**：密钥只能被引用（`SecretRef`），创建/更新时解析并**绑定具体版本**：
  - 引用缺失或版本失效 → `ErrSecretRef` 明确失败，不退化为默认值；
  - 轮换后新建环境使用新版本，已存在环境继续读绑定时固定的版本；
  - 密钥值只通过进程环境变量传递，**绝不**出现在流水日志或沙箱产物中
    （流水只记录 `key@version -> $ENV`）。
- **并发安全**：所有状态变更在单互斥锁下进行；销毁中的环境对 `Get` 不可见
  （返回 `ErrDestroying`）；并发 `Destroy` 只生效一次，重复调用安全返回。

## 错误分类（可区分的原因）

| 错误 | 含义 |
|---|---|
| `ErrInvalidSpec` | 描述非法（空名、空命令、未知依赖……） |
| `ErrDependencyCycle` | 依赖图有环 |
| `ErrConflict` | 与已有环境冲突（保留给扩展策略） |
| `ErrCapacity` | 环境数/服务数/端口达到上限 |
| `ErrSecretRef` | 密钥引用缺失或版本失效 |
| `ErrDestroying` | 环境正在销毁，对访问方不可见 |
| `ErrRollbackFailed` | 回滚/清理失败，可对账重试 |
| `ErrNotConvergent` | 对账无法收敛 |

均可用 `errors.Is` 判定。

## 本地验证

```bash
# 全部测试（需 CGO 以启用竞态检测）
CGO_ENABLED=1 go test ./... -race -count=1 -v
```

测试覆盖：并发调谐（并发 Apply/Destroy/Get/Reconcile）、依赖失败阻塞下游、
密钥缺失/版本失效/轮换绑定/防泄漏、端口冲突重分配与池耗尽、容量拒绝与排队、
到期回收竞争、回收失败重试（不重复释放）、回滚失败对账重试、外部漂移修复、
更新收敛幂等。测试日志通过 `Ledger` 打印判定依据与每一次资源分配/释放流水，例如：

```
---- 资源分配/释放流水 ----
[alloc-port] env=sec svc=web http=31000
[bind-secret] env=sec svc=web ghost@0 -> $TOKEN   # 只有引用，没有值
[release-port] env=sec svc=web http=31000 (rollback)
```
