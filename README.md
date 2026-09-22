# preview-env-orchestrator-go

本地预览环境的**编排与生命周期管理**组件：用本地进程与文件系统模拟隔离沙箱，
把声明式的环境描述收敛成可运行、可访问、可变更、可回收的预览环境实例，
并在异常与并发场景下给出确定且可解释的结果。

## 能力一览

- **声明式、幂等**：相同描述重复提交得到同一实例（规范化 `SpecHash`）；更新按差异
  收敛（受控重建），不累积副作用；非法描述与环依赖被明确拒绝（可区分原因）。
- **依赖与启动顺序**：拓扑排序启动；上游未就绪不启动下游；上游失败时下游标记
  `Blocked`（不静默跳过）；依赖图成环直接拒绝并报告环路径。
- **独占资源与配额**：可配置端口范围与环境数上限；固定端口冲突即失败、动态端口耗尽
  即报错；满员支持 `Reject` 或 FIFO `Queue`（名额直接转交，不无界占用）。
- **密钥引用与轮换**：密钥只引用、不落盘、不进日志；缺失/版本失效明确失败而非默认值；
  轮换后新环境用新版本，既有环境（含对账重启）保持绑定时的版本。
- **并发安全**：同环境调谐/创建/销毁串行化，创建按名去重；访问方永远拿不到半创建或
  正在销毁的实例；重复销毁不重复释放进程/端口/名额。
- **到期回收**：临时环境 TTL 到期自动回收，失败可重试、不误删、不重复释放。
- **回滚与对账**：任一步骤失败回滚已分配资源，回滚失败可区分且可重试；
  崩溃后可从快照恢复簿记；外部改动导致的偏离可修复或明确报告“不可收敛”。

## 快速开始

```bash
go run ./cmd/orchestrator          # 端到端演示
go test ./...                       # 全部测试
CGO_ENABLED=1 go test -race ./...   # 竞态检测
```

## 最小示例

```go
secrets := secret.NewStore()
secrets.Put("db-password", "******")

e, _ := engine.NewAndRecover(engine.Config{
    StateDir: ".state", MaxEnvironments: 4,
    PortMin: 20000, PortMax: 20100,
    LimitPolicy: engine.PolicyReject, SecretStore: secrets,
})

spec := domain.EnvironmentSpec{
    Name: "pr-123", TTL: 30 * time.Minute,
    Services: []domain.ServiceSpec{
        {Name: "db",  Image: "postgres", Ports: []domain.PortSpec{{Name: "pg"}},
         Secrets: []domain.SecretRef{{Name: "db-password"}}},
        {Name: "api", Image: "svc", DependsOn: []string{"db"}},
    },
}
env, err := e.Apply(context.Background(), spec)   // 创建/更新（幂等）
e.Get("pr-123")                                    // 访问（销毁中不可见）
e.ReconcileAll(context.Background(), time.Now())   // 对账 + 到期回收
e.Delete(context.Background(), "pr-123")           // 销毁（幂等、可重试）
```

## 文档

- 环境模型、调谐规则、资源/密钥约束、并发与恢复语义、本地验证方法：
  见 [`docs/design.md`](docs/design.md)。
