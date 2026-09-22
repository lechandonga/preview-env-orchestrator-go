package engine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"
)

func TestApplyCreatesReadyEnvironment(t *testing.T) {
	h := newHarness(t, 4, 31000, 31020, enginePolicyReject())
	env := h.apply(basicSpec("e1"))
	if env.Phase != domain.PhaseReady {
		t.Fatalf("phase=%s, want Ready: %s", env.Phase, serviceSummary(env))
	}
	if len(env.Services) != 3 {
		t.Fatalf("want 3 services, got %d", len(env.Services))
	}
	for name, st := range env.Services {
		if st.Phase != domain.ServiceReady {
			t.Fatalf("service %s phase=%s, want Ready", name, st.Phase)
		}
		if st.PID <= 0 {
			t.Fatalf("service %s has no pid", name)
		}
	}
	if env.Services["db"].Ports[0].Port == 0 {
		t.Fatal("db port was not allocated")
	}
	h.assertPhase("e1", domain.PhaseReady)
	t.Logf("judgement: e1 Ready; ports db=%d api=%d; pids recorded",
		env.Services["db"].Ports[0].Port, env.Services["api"].Ports[0].Port)

	// 清理。
	if err := h.engine.Delete(context.Background(), "e1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

func TestApplyIdempotentSameSpec(t *testing.T) {
	h := newHarness(t, 4, 31000, 31020, enginePolicyReject())
	first := h.apply(basicSpec("dup"))
	second := h.apply(basicSpec("dup"))
	if first.Generation != second.Generation {
		t.Fatalf("re-apply changed generation %d -> %d", first.Generation, second.Generation)
	}
	if first.Services["db"].PID != second.Services["db"].PID {
		t.Fatalf("re-apply restarted db pid %d -> %d (expected no-op)", first.Services["db"].PID, second.Services["db"].PID)
	}
	t.Logf("judgement: identical spec re-submitted => same instance generation=%d pid=%d (no side effects)",
		second.Generation, second.Services["db"].PID)
	_ = h.engine.Delete(context.Background(), "dup")
}

func TestApplyValidationAndCycle(t *testing.T) {
	h := newHarness(t, 4, 31000, 31020, enginePolicyReject())

	bad := domain.EnvironmentSpec{Name: "bad", Services: []domain.ServiceSpec{
		{Name: "a", Image: "", DependsOn: []string{"ghost"}},
	}}
	_, err := h.engine.Apply(context.Background(), bad)
	var ve *domain.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("want ValidationError, got %T %v", err, err)
	}
	t.Logf("judgement: invalid spec rejected with reasons=%v (no resources allocated)", ve.Reasons)

	cyclic := domain.EnvironmentSpec{Name: "cyc", Services: []domain.ServiceSpec{
		{Name: "a", Image: "i", DependsOn: []string{"b"}},
		{Name: "b", Image: "i", DependsOn: []string{"a"}},
	}}
	_, err = h.engine.Apply(context.Background(), cyclic)
	var ce *domain.CycleError
	if !errors.As(err, &ce) {
		t.Fatalf("want CycleError, got %T %v", err, err)
	}
	t.Logf("judgement: cycle rejected with path=%v", ce.Cycle)

	if _, ok := h.engine.Get("bad"); ok {
		t.Fatal("invalid environment must not be visible after rejection")
	}
	if _, ok := h.engine.Get("cyc"); ok {
		t.Fatal("cyclic environment must not be visible after rejection")
	}
}

func TestApplyMissingSecretFails(t *testing.T) {
	h := newHarness(t, 4, 31000, 31020, enginePolicyReject())
	spec := basicSpec("sec")
	spec.Services[0].Secrets = []domain.SecretRef{{Name: "missing-key"}}
	_, err := h.engine.Apply(context.Background(), spec)
	var se *domain.SecretError
	if !errors.As(err, &se) {
		t.Fatalf("want SecretError, got %T %v", err, err)
	}
	t.Logf("judgement: missing secret rejected before allocation: %v", se)
	if _, ok := h.engine.Get("sec"); ok {
		t.Fatal("environment with unresolvable secret must not be created")
	}
	// 密钥补齐后应能正常创建（不是永久失败）。
	if _, err := h.store.Put("missing-key", "s3cr3t"); err != nil {
		t.Fatal(err)
	}
	env := h.apply(spec)
	if env.Phase != domain.PhaseReady {
		t.Fatalf("after secret created, want Ready, got %s", env.Phase)
	}
	// 密钥值绝不能出现在持久化快照中。
	got, _ := h.engine.Get("sec")
	for name, st := range got.Services {
		for _, b := range st.Secrets {
			if b.Name == "missing-key" && b.Version != 1 {
				t.Fatalf("service %s bound wrong version %d", name, b.Version)
			}
		}
	}
	if data := readFile(t, h.dir, "sec"); contains(data, "s3cr3t") {
		t.Fatalf("secret value leaked into snapshot: %s", data)
	}
	t.Log("judgement: secret value absent from snapshot; binding pinned to v1")
	_ = h.engine.Delete(context.Background(), "sec")
}

func TestApplyUpstreamFailureBlocksDownstream(t *testing.T) {
	h := newHarness(t, 4, 31000, 31020, enginePolicyReject())
	spec := basicSpec("depfail")
	spec.Services[0].Image = "fail" // db 启动失败
	env, err := h.engine.Apply(context.Background(), spec)
	if err != nil {
		t.Fatalf("apply should converge to Degraded, got err=%v", err)
	}
	if env.Phase != domain.PhaseDegraded {
		t.Fatalf("want Degraded, got %s", env.Phase)
	}
	if env.Services["db"].Phase != domain.ServiceFailed {
		t.Fatalf("db want Failed, got %s", env.Services["db"].Phase)
	}
	if env.Services["api"].Phase != domain.ServiceBlocked {
		t.Fatalf("api want Blocked (upstream failed), got %s: %s", env.Services["api"].Phase, env.Services["api"].Reason)
	}
	if env.Services["web"].Phase != domain.ServiceBlocked {
		t.Fatalf("web want Blocked (transitive), got %s: %s", env.Services["web"].Phase, env.Services["web"].Reason)
	}
	t.Logf("judgement: db=Failed; api=%s(%s); web=%s(%s); unrelated resources kept",
		env.Services["api"].Phase, env.Services["api"].Reason,
		env.Services["web"].Phase, env.Services["web"].Reason)
	// 失败服务没有残留端口。
	if len(env.Services["db"].Ports) != 0 {
		t.Fatalf("failed db must not hold ports, got %v", env.Services["db"].Ports)
	}
	_ = h.engine.Delete(context.Background(), "depfail")
}

func TestApplyFixedPortConflict(t *testing.T) {
	h := newHarness(t, 4, 31000, 31020, enginePolicyReject())
	one := basicSpec("one")
	one.Services = one.Services[:1] // 只留 db
	one.Services[0].Ports = []domain.PortSpec{{Name: "pg", Fixed: 31005}}
	h.apply(one)

	two := basicSpec("two")
	two.Services = two.Services[:1]
	two.Services[0].Name = "other"
	two.Services[0].Ports = []domain.PortSpec{{Name: "pg", Fixed: 31005}}
	env, err := h.engine.Apply(context.Background(), two)
	if err != nil {
		t.Fatalf("fixed port clash should degrade (not rollback other env), got err=%v", err)
	}
	if env.Services["other"].Phase != domain.ServiceFailed {
		t.Fatalf("other want Failed due to fixed port conflict, got %s", env.Services["other"].Phase)
	}
	// 第一个环境仍然可用，端口未被抢占。
	if got := h.assertPhase("one", domain.PhaseReady); got.Services["db"].Ports[0].Port != 31005 {
		t.Fatal("env one lost its fixed port")
	}
	t.Log("judgement: fixed port conflict rejected for new env without disrupting existing env")
	_ = h.engine.Delete(context.Background(), "one")
	_ = h.engine.Delete(context.Background(), "two")
}

func TestApplyQuotaLimitReject(t *testing.T) {
	h := newHarness(t, 1, 31000, 31020, enginePolicyReject())
	h.apply(basicSpec("q1"))
	_, err := h.engine.Apply(context.Background(), basicSpec("q2"))
	var qe *domain.QuotaExceededError
	if !errors.As(err, &qe) {
		t.Fatalf("want QuotaExceededError, got %T %v", err, err)
	}
	if _, ok := h.engine.Get("q2"); ok {
		t.Fatal("rejected env must not exist")
	}
	t.Logf("judgement: quota(max=1) enforced, second env rejected: %v", qe)
	_ = h.engine.Delete(context.Background(), "q1")
	// 删除后名额释放，可再创建。
	h.apply(basicSpec("q3"))
	h.assertPhase("q3", domain.PhaseReady)
	t.Log("judgement: quota released after delete allows new environment")
	_ = h.engine.Delete(context.Background(), "q3")
}

func TestApplySecretRotationPinning(t *testing.T) {
	h := newHarness(t, 4, 31000, 31020, enginePolicyReject())
	if _, err := h.store.Put("k", "v1-secret"); err != nil {
		t.Fatal(err)
	}
	spec := basicSpec("rot")
	spec.Services[0].Secrets = []domain.SecretRef{{Name: "k"}} // 浮动：绑定时固化
	env := h.apply(spec)
	if env.Services["db"].Secrets[0].Version != 1 {
		t.Fatal("expected pin to v1")
	}
	if _, err := h.store.Rotate("k", "v2-secret"); err != nil {
		t.Fatal(err)
	}
	// 已存在环境继续读到绑定版本（对账重启也用固化版本）：杀死 db 进程模拟外部改动。
	killPid(t, env.Services["db"].PID)
	rec, err := h.engine.Reconcile(context.Background(), "rot")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if rec.Services["db"].Secrets[0].Version != 1 {
		t.Fatalf("existing env must stay pinned to v1, got %d", rec.Services["db"].Secrets[0].Version)
	}
	if rec.Services["db"].Phase != domain.ServiceReady {
		t.Fatalf("db should be restarted Ready, got %s reason=%q", rec.Services["db"].Phase, rec.Services["db"].Reason)
	}
	// 新环境（显式用浮动引用）应绑定 v2。
	spec2 := basicSpec("rot2")
	spec2.Services[0].Secrets = []domain.SecretRef{{Name: "k"}}
	env2 := h.apply(spec2)
	if env2.Services["db"].Secrets[0].Version != 2 {
		t.Fatalf("new env must bind rotated v2, got %d", env2.Services["db"].Secrets[0].Version)
	}
	t.Log("judgement: existing env pinned to v1 across restart; new env binds v2 after rotation")
	_ = h.engine.Delete(context.Background(), "rot")
	_ = h.engine.Delete(context.Background(), "rot2")
}

func TestApplyTTLExpiry(t *testing.T) {
	h := newHarness(t, 4, 31000, 31020, enginePolicyReject())
	spec := basicSpec("ttl")
	spec.TTL = 50 * time.Millisecond
	h.apply(spec)
	if _, ok := h.engine.Get("ttl"); !ok {
		t.Fatal("ttl env should be visible before expiry")
	}
	time.Sleep(80 * time.Millisecond)
	if err := h.engine.ReconcileAll(context.Background(), time.Now()); err != nil {
		t.Fatalf("reconcile all: %v", err)
	}
	if _, ok := h.engine.Get("ttl"); ok {
		t.Fatal("expired env must be reclaimed")
	}
	t.Log("judgement: temporary env reclaimed after TTL expiry")
}
