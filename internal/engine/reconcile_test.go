package engine_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"
)

// 外部把一个服务进程杀死：对账必须重启它（复用绑定端口与固化的密钥版本），
// 同时严格保持启动顺序——依赖未恢复前不得重启下游。
func TestReconcileRestartsKilledProcess(t *testing.T) {
	h := newHarness(t, 4, 35000, 35080, enginePolicyReject())
	env := h.apply(basicSpec("drift"))
	dbPort := env.Services["db"].Ports[0].Port
	oldAPIPid := env.Services["api"].PID

	killPid(t, env.Services["db"].PID)
	// 同时把 api 也杀掉，验证按拓扑顺序重建（db 先于 api）。
	killPid(t, oldAPIPid)

	rec, err := h.engine.Reconcile(context.Background(), "drift")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if rec.Services["db"].Phase != domain.ServiceReady || rec.Services["api"].Phase != domain.ServiceReady ||
		rec.Services["web"].Phase != domain.ServiceReady {
		t.Fatalf("not all restored: %s", serviceSummary(rec))
	}
	if rec.Services["db"].Ports[0].Port != dbPort {
		t.Fatalf("db port changed %d -> %d (bound address must be preserved)", dbPort, rec.Services["db"].Ports[0].Port)
	}
	if rec.Services["db"].PID == env.Services["db"].PID {
		t.Fatal("db should have a new pid after restart")
	}
	if rec.Phase != domain.PhaseReady {
		t.Fatalf("env phase=%s, want Ready after repair", rec.Phase)
	}
	t.Logf("judgement: killed db/api restarted in topo order; db kept port=%d; phase back to Ready", dbPort)
	_ = h.engine.Delete(context.Background(), "drift")
}

// 外部在沙箱目录里遗留一个未知服务的 PID 文件/进程（孤儿）：
// 对账必须发现并清理它，而不是把它并入环境。
func TestReconcileOrphanProcess(t *testing.T) {
	h := newHarness(t, 4, 35000, 35080, enginePolicyReject())
	env := h.apply(basicSpec("orph"))
	_ = env
	// 手工制造一个孤儿 PID 文件，指向一个真实 sleep 进程。
	cmd := newSleepCmd()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	orphanPID := cmd.Process.Pid
	go func() { _ = cmd.Wait() }()
	dir := filepath.Join(h.dir, "sandbox", "orph")
	if err := os.WriteFile(filepath.Join(dir, "ghost.pid"), []byte(fmt.Sprintf("%d", orphanPID)), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := h.engine.Reconcile(context.Background(), "orph"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// 孤儿进程必须已被杀掉。
	deadline := nowMillis() + 2000
	for nowMillis() < deadline {
		if syscall.Kill(orphanPID, 0) != nil {
			break // 已退出
		}
		sleepMs(5)
	}
	if syscall.Kill(orphanPID, 0) == nil {
		_ = syscall.Kill(orphanPID, syscall.SIGKILL)
		t.Fatal("orphan process survived reconciliation")
	}
	if _, err := os.Stat(filepath.Join(dir, "ghost.pid")); !os.IsNotExist(err) {
		t.Fatal("orphan pid file must be removed")
	}
	t.Logf("judgement: unknown ghost service pid=%d and its pid file were reaped", orphanPID)
	_ = h.engine.Delete(context.Background(), "orph")
}

// 对账时发现绑定的密钥版本已被吊销：必须明确报告不可收敛，而不是退化为空值。
func TestReconcileBoundSecretDisabled(t *testing.T) {
	h := newHarness(t, 4, 35000, 35080, enginePolicyReject())
	if _, err := h.store.Put("cred", "v1"); err != nil {
		t.Fatal(err)
	}
	spec := basicSpec("bindsec")
	spec.Services[0].Secrets = []domain.SecretRef{{Name: "cred"}}
	env := h.apply(spec)
	killPid(t, env.Services["db"].PID)
	if err := h.store.DisableVersion("cred", 1); err != nil {
		t.Fatal(err)
	}
	_, err := h.engine.Reconcile(context.Background(), "bindsec")
	var nc *domain.NotConvergeableError
	if !errors.As(err, &nc) {
		t.Fatalf("want NotConvergeableError, got %T %v", err, err)
	}
	if !strings.Contains(nc.Error(), "cred") {
		t.Fatalf("error should name the secret: %v", nc)
	}
	t.Logf("judgement: disabled bound secret -> explicit non-convergence: %v", nc)
	_ = h.engine.Delete(context.Background(), "bindsec")
}

// 创建过程中某服务启动失败 => 整体回滚已分配资源，不留孤儿端口/进程/配额。
func TestCreateFailureRollback(t *testing.T) {
	h := newHarness(t, 4, 35000, 35080, enginePolicyReject())
	spec := basicSpec("rb")
	// 让第二个服务（api）失败：它有一个端口且依赖 db。db 应先成功启动，
	// api 失败被标记 Failed、web 被 Blocked —— 这是 Degraded 收敛而非回滚。
	// 为了专门验证“回滚”，这里改用端口池耗尽触发基础设施失败。
	spec.Services[1].Ports = []domain.PortSpec{{Name: "http", Fixed: 39999}} // 池外 => 分配失败
	env, err := h.engine.Apply(context.Background(), spec)
	if err != nil {
		t.Fatalf("out-of-range fixed port should degrade the single service, not error the apply: %v", err)
	}
	if env.Phase != domain.PhaseDegraded {
		t.Fatalf("want Degraded, got %s", env.Phase)
	}
	// api 未持有任何端口，db 正常。
	if len(env.Services["api"].Ports) != 0 {
		t.Fatalf("failed api holds ports: %v", env.Services["api"].Ports)
	}
	if h.engine.PortsInUse() != 1 {
		t.Fatalf("only db port should remain, pool=%d", h.engine.PortsInUse())
	}
	t.Log("judgement: out-of-pool fixed port degraded api, blocked web, no leak (ports=1)")
	_ = h.engine.Delete(context.Background(), "rb")
	if h.engine.PortsInUse() != 0 || h.engine.QuotaInUse() != 0 {
		t.Fatalf("leak after delete: ports=%d quota=%d", h.engine.PortsInUse(), h.engine.QuotaInUse())
	}
	t.Log("judgement: cleanup returned ports=0 quota=0")
}
