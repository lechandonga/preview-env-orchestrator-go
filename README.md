# preview-env-orchestrator-go

本地预览环境编排与生命周期管理组件。用**本地进程 + 文件系统**模拟隔离沙箱，
把声明式的环境描述（`EnvSpec`）收敛成可运行的预览环境实例，使创建、访问、
变更与回收在异常与并发场景下都有**确定且可解释**的结果。

状态落盘在本地文件系统（`StateDir`，默认 `WorkDir/state`），因此：
引擎进程正常退出或被强杀后，用同一份状态重新 `New` 即可**接管**全部环境；
同一台机器上可以同时运行多个引擎实例，它们共用同一份状态、同一套端口与配额。

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
- **并发安全**：状态变更在"进程内互斥锁 + 跨进程 `flock` 独占锁"下进行，
  每次进入临界区都从磁盘重载权威状态，支持多实例共用；销毁中的环境对 `Get`
  不可见（返回 `ErrDestroying`）；并发/跨实例 `Destroy` 只生效一次，重复调用安全返回。

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

## 持久化与多实例

### 落盘都存了什么

所有状态都在 `StateDir` 下，全部写入都是 `temp 文件 + rename` 原子替换：

| 文件 | 内容 |
|---|---|
| `state/env-<名称>.json` | 每个环境一份：描述 `EnvSpec`、指纹、生命周期阶段（`creating`/`active`/`destroying`）、状态、创建/到期时间；每个服务的指纹、**进程 PID**、端口名→端口、**密钥引用（key@version）**、blocked/failed 标记、上次清理失败原因 |
| `state/ports.json` | 全局端口持有者表（端口 → 环境名），是端口分配的权威依据 |
| `state/ledger.log` | 共享资源流水（每行一条 JSON：判定依据、每次端口分配/释放、进程启停、密钥**引用**绑定、对账与回收） |
| `state/engine.lock` | 跨进程 `flock` 文件锁（不存业务数据） |

沙箱本体仍在 `WorkDir/<环境名>/`（工作目录与服务日志）。

### 密钥为什么只存引用

状态文件可能被备份、拷贝、留在磁盘上很久，而密钥值一旦落盘就无法保证不扩散，
所以落盘记录里**只有 `key@version` 引用**：密钥值只在（重新）启动进程时从
`secrets.Store` 实时取出、经进程环境变量传递，从不写入状态、流水或沙箱日志。
这同时保证了重启后的绑定语义：记录里的版本号就是该环境当初绑定的版本，
轮换出新版本后旧环境仍读旧版本；绑定版本被吊销或引用消失时，对账重启会
以 `ErrNotConvergent`（内含 `ErrSecretRef`）**明确失败**，绝不静默拿最新版本顶上。

### 崩溃恢复语义

`New` 打开状态时持独占锁执行接管：

- `active` 环境：恢复内存视图，按记录的 PID 用 `kill(pid, 0)` **接管仍存活的
  服务进程**并继续观察（`reattach-lost` 标记已消失的进程，等 `Reconcile` 修复）；
  端口表从 `ports.json` 重建，仍被占用的端口不会被再次分配。
- `creating` 记录（创建到一半被杀）：判定为孤儿，释放其端口、停残留进程、
  删沙箱与记录（`orphan-abandoned`），不占配额、不留孤儿资源。
- `destroying` 记录（销毁到一半被杀）：续做完销毁——停进程、释放端口、删沙箱
  与记录（`destroy-resume`）。所有释放步骤幂等（只有持有者能释放端口，
  且重复调用不生效），因此销毁与到期回收跨重启也只真正生效一次。
- 服务进程被外部杀掉：`Reconcile` 检测 `proc-dead` 漂移并用**原密钥绑定**重启；
  修不了（绑定失效、端口无法满足）返回 `ErrNotConvergent`。

### 多实例共用

多个 `New(同一 StateDir)` 的实例（同机多进程，或同进程内多个句柄）：
所有变更操作先取进程内锁、再取 `flock` 独占锁，并在临界区开头从磁盘
**重载权威状态**，因此环境数上限、端口、配额是全局一致的——不会有两个实例
把同一端口分给不同环境，也绕不过 `MaxEnvs`。一个实例建的环境另一个实例立即可见、
可销毁；某实例被强杀后，其他实例的下一次操作或新实例启动时安全接管其环境。
读操作（`Get`/`List`）使用共享锁，不阻塞彼此。

### 兼容性

- 记录带 `version` 字段（当前为 1）。新版可以直接加载 v1 记录；
  缺少阶段字段（`phase`）的更旧记录按 `active` 处理：环境正常接管，
  其存活进程与端口占用都会被重新确认。
- 配置（端口范围、上限等）不从状态读取，仍由 `Config` 提供；用**相同或更大的
  端口范围**指向旧状态是安全的。缩小范围时，落在新范围外的历史端口会被忽略，
  相应环境应先销毁再重建。

### 本地复现新增能力

```bash
# 只跑本次新增的恢复/多实例场景（含 -race 竞态检测）
CGO_ENABLED=1 go test ./orchestrator/ -race -count=1 -v \
  -run 'TestRestart|TestOrphan|TestMulti|TestHardKill|TestPersistedState'
```

- `TestRestartRecoveryGraceful`：`Shutdown()`（不销毁）后新实例 `New`，
  环境/状态/端口/配额/流水全在，仍存活的服务被接管，并能跨实例销毁（只生效一次）。
- `TestRestartHardKillTakeover`：通过测试二进制子进程（`TestHelperProcess`）
  真实 `SIGKILL` 引擎，验证服务进程存活、被新实例接管；再外部杀掉服务，
  `Reconcile` 用原端口/原密钥绑定重启修复。全程不需要外部服务或真实凭据。
- `TestRestartSecretRotationBinding`：重启后轮换密钥，旧环境坚持 v1、
  新环境用 v2；v1 引用失效后对账返回 `ErrNotConvergent`+`ErrSecretRef`。
- `TestMultiInstanceSharedState`：两个实例并发创建/销毁/查询/补位，
  断言上限一致、端口无重复分配、跨实例可见可销毁。
- `TestOrphanCreateCleanupOnRestart` / `TestHardKillMidDestroyAfterTakeover`：
  创建一半、销毁一半的崩溃记录在启动时分别被清理 / 续做。
- `TestRestartExpiryReap`、`TestPersistedStateHasNoSecretValues`：
  到期回收跨重启只生效一次；落盘文件、流水、沙箱产物均无密钥明文。

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
