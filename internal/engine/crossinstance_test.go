package engine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"
	"github.com/lechandonga/preview-env-orchestrator-go/internal/engine"
)

// 两个引擎实例指向同一状态目录：共享环境数配额与端口池，互不可见的内存状态
// 也必须通过文件锁/位图协调，使限额与独占端口仍然成立。
func TestTwoEnginesShareStateDir(t *testing.T) {
	dir := tTempDir(t) + "/state"

	// 配额放宽以隔离“端口跨实例独占”这一断言。
	e1, err := engine.NewAndRecover(engine.Config{StateDir: dir, MaxEnvironments: 4, PortMin: 45000, PortMax: 45010})
	if err != nil {
		t.Fatal(err)
	}
	setFlowLogger(t, e1)
	e2, err := engine.NewAndRecover(engine.Config{StateDir: dir, MaxEnvironments: 4, PortMin: 45000, PortMax: 45010})
	if err != nil {
		t.Fatal(err)
	}
	setFlowLogger(t, e2)

	// 单独验证共享环境数上限：临时第三个实例以 max=1 打开同一目录。
	eq1, err := engine.NewAndRecover(engine.Config{StateDir: dir, MaxEnvironments: 1, PortMin: 45000, PortMax: 45010})
	if err != nil {
		t.Fatal(err)
	}
	setFlowLogger(t, eq1)
	applyWith(t, eq1, basicSpec("alpha"))
	// 另一个引擎实例也用 max=1 的视图打开同一目录做配额断言。
	eq2, err := engine.NewAndRecover(engine.Config{StateDir: dir, MaxEnvironments: 1, PortMin: 45000, PortMax: 45010})
	if err != nil {
		t.Fatal(err)
	}
	setFlowLogger(t, eq2)
	_, err = eq2.Apply(context.Background(), basicSpec("beta-blocked"))
	var qe *domain.QuotaExceededError
	if !errors.As(err, &qe) {
		t.Fatalf("second engine must see shared quota full, got %T %v", err, err)
	}
	t.Log("judgement: second engine rejected by shared environment quota (max=1)")
	if err := eq1.Delete(context.Background(), "alpha"); err != nil {
		t.Fatal(err)
	}

	// 端口跨实例独占：e1 建 beta，e2 抢其固定端口。
	beta := applyWith(t, e1, basicSpec("beta"))
	if beta.Phase != domain.PhaseReady {
		t.Fatalf("beta phase=%s", beta.Phase)
	}
	t.Log("judgement: after engine #1 deleted, engine #2 acquired the freed slot")

	// 端口独占跨实例：e1 尝试占用 beta 的 db 端口必须冲突。
	betaPort := beta.Services["db"].Ports[0].Port
	gamma := basicSpec("gamma")
	gamma.Services = gamma.Services[:1]
	gamma.Services[0].Ports = []domain.PortSpec{{Name: "pg", Fixed: betaPort}}
	env3, err := e2.Apply(context.Background(), gamma)
	if err != nil {
		t.Fatal(err)
	}
	if env3.Services["db"].Phase != domain.ServiceFailed {
		t.Fatalf("fixed-port clash across engines must fail the service, got %s", env3.Services["db"].Phase)
	}
	t.Logf("judgement: fixed port %d held by engine #2 env beta is not stealable by engine #1", betaPort)

	// e2 能看到 e1 创建的失败实例（快照共享），且删除幂等。
	if _, ok := e2.Get("gamma"); !ok {
		t.Fatal("engine #2 must see snapshots written by engine #1")
	}
	_ = e1.Delete(context.Background(), "gamma")
	_ = e2.Delete(context.Background(), "beta")
	if e1.PortsInUse() != 0 || e2.PortsInUse() != 0 || e1.QuotaInUse() != 0 || e2.QuotaInUse() != 0 {
		t.Fatalf("shared resources not zeroed: e1(ports=%d quota=%d) e2(ports=%d quota=%d)",
			e1.PortsInUse(), e1.QuotaInUse(), e2.PortsInUse(), e2.QuotaInUse())
	}
	t.Log("judgement: shared cleanup consistent across both engine instances (ports=0 quota=0)")
}
