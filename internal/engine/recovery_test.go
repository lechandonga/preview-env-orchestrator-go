package engine_test

import (
	"context"
	"testing"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"
	"github.com/lechandonga/preview-env-orchestrator-go/internal/engine"
	"github.com/lechandonga/preview-env-orchestrator-go/internal/secret"
)

// 引擎重启：从持久化快照重建端口/配额簿记；外部杀死的进程被标记，
// 随后 Reconcile 能修复并继续读到绑定的密钥版本。
func TestRecoveryAcrossRestart(t *testing.T) {
	dir := tTempDir(t)
	stateDir := dir + "/state"
	ss := secret.NewStore()
	_, _ = ss.Put("k", "v1")

	e1, err := engine.New(engine.Config{StateDir: stateDir, MaxEnvironments: 3, PortMin: 38000, PortMax: 38050, SecretStore: ss})
	if err != nil {
		t.Fatal(err)
	}
	setFlowLogger(t, e1)
	spec := basicSpec("restart")
	spec.Services[0].Secrets = []domain.SecretRef{{Name: "k"}}
	env := applyWith(t, e1, spec)
	dbPort := env.Services["db"].Ports[0].Port
	killPid(t, env.Services["db"].PID)

	// 用同一目录与同一密钥存储重建引擎（模拟崩溃后重启）。
	e2, err := engine.NewAndRecover(engine.Config{StateDir: stateDir, MaxEnvironments: 3, PortMin: 38000, PortMax: 38050, SecretStore: ss})
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	setFlowLogger(t, e2)
	if e2.QuotaInUse() != 1 {
		t.Fatalf("quota after restart=%d, want 1", e2.QuotaInUse())
	}
	if e2.PortsInUse() != 2 { // db + api 两个端口
		t.Fatalf("ports after restart=%d, want 2", e2.PortsInUse())
	}
	got, ok := e2.Get("restart")
	if !ok {
		t.Fatal("env missing after restart")
	}
	if got.Services["db"].Phase != domain.ServicePending {
		t.Fatalf("dead db after restart want Pending, got %s", got.Services["db"].Phase)
	}
	if got.Phase != domain.PhaseDegraded {
		t.Fatalf("env after restart want Degraded, got %s", got.Phase)
	}
	t.Logf("judgement: bookkeeping restored quota=1 ports=2; dead db marked Pending; env Degraded")

	// 对账修复：db 用固化的 v1 重启，端口保持。
	rec, err := e2.Reconcile(context.Background(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Services["db"].Phase != domain.ServiceReady {
		t.Fatalf("db after reconcile=%s", rec.Services["db"].Phase)
	}
	if rec.Services["db"].Ports[0].Port != dbPort {
		t.Fatalf("db port drifted %d -> %d", dbPort, rec.Services["db"].Ports[0].Port)
	}
	if rec.Services["db"].Secrets[0].Version != 1 {
		t.Fatal("bound secret version lost across restart")
	}
	if rec.Phase != domain.PhaseReady {
		t.Fatalf("phase=%s after repair", rec.Phase)
	}
	t.Log("judgement: post-restart reconcile restarted db with bound port and pinned secret v1")
	_ = e2.Delete(context.Background(), "restart")
	if e2.QuotaInUse() != 0 || e2.PortsInUse() != 0 {
		t.Fatal("resources not zeroed after delete on recovered engine")
	}
}

// 删除在中途崩溃（快照为 Deleting）：重启恢复后对账应继续完成删除并释放簿记。
func TestRecoveryResumesInterruptedDelete(t *testing.T) {
	dir := tTempDir(t)
	stateDir := dir + "/state"
	e1, err := engine.New(engine.Config{StateDir: stateDir, MaxEnvironments: 3, PortMin: 38100, PortMax: 38150})
	if err != nil {
		t.Fatal(err)
	}
	setFlowLogger(t, e1)
	env := applyWith(t, e1, basicSpec("delint"))
	_ = env

	// 手工把快照标记为 Deleting（模拟删除在标记后、清理前崩溃），进程仍在。
	got, ok, _ := snapshotGet(e1, "delint")
	if !ok {
		t.Fatal("snapshot missing")
	}
	got.Phase = domain.PhaseDeleting
	if err := snapshotPersist(e1, got); err != nil {
		t.Fatal(err)
	}

	e2, err := engine.NewAndRecover(engine.Config{StateDir: stateDir, MaxEnvironments: 3, PortMin: 38100, PortMax: 38150})
	if err != nil {
		t.Fatal(err)
	}
	setFlowLogger(t, e2)
	// 恢复簿记时 Deleting 仍占名额（held 计数），Reconcile 完成删除后应归零。
	if err := e2.ReconcileAll(context.Background(), nowTime()); err != nil {
		t.Fatalf("reconcile all: %v", err)
	}
	if _, ok := e2.Get("delint"); ok {
		t.Fatal("interrupted delete must finish after restart")
	}
	if e2.QuotaInUse() != 0 || e2.PortsInUse() != 0 {
		t.Fatalf("resources leaked across interrupted delete: quota=%d ports=%d", e2.QuotaInUse(), e2.PortsInUse())
	}
	t.Log("judgement: Deleting snapshot resumed on restart; quota=0 ports=0 afterwards")
}
