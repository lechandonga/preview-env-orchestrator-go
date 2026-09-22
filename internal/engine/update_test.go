package engine_test

import (
	"context"
	"sync"
	"testing"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"
)

// 更新声明必须收敛到期望状态：移除的服务进程/端口被释放，新增服务按序启动；
// 不得不断追加进程与端口（无副作用累积）。
func TestUpdateConvergesWithoutAccumulating(t *testing.T) {
	h := newHarness(t, 4, 41000, 41080, enginePolicyReject())
	env := h.apply(basicSpec("upd"))
	oldPIDs := map[string]int{}
	for n, st := range env.Services {
		oldPIDs[n] = st.PID
	}
	if h.engine.PortsInUse() != 2 {
		t.Fatalf("initial ports=%d, want 2", h.engine.PortsInUse())
	}

	// 更新：删除 web，新增 cache（无依赖）；db/api 保留。
	spec2 := basicSpec("upd")
	spec2.Services = []domain.ServiceSpec{
		{Name: "db", Image: "postgres", Ports: []domain.PortSpec{{Name: "pg"}}},
		{Name: "cache", Image: "redis"},
		{Name: "api", Image: "svc", DependsOn: []string{"db", "cache"}, Ports: []domain.PortSpec{{Name: "http"}}},
	}
	env2, err := h.engine.Apply(context.Background(), spec2)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if env2.Generation != 2 {
		t.Fatalf("generation=%d, want 2 after update", env2.Generation)
	}
	if _, has := env2.Services["web"]; has {
		t.Fatal("removed service web must not exist after converge")
	}
	for _, n := range []string{"db", "api", "cache"} {
		if env2.Services[n].Phase != domain.ServiceReady {
			t.Fatalf("%s not Ready: %s", n, env2.Services[n].Phase)
		}
	}
	// 端口账：db 1 + api 1 = 2（cache/web 无端口）。
	if h.engine.PortsInUse() != 2 {
		t.Fatalf("ports after update=%d, want 2 (no accumulation)", h.engine.PortsInUse())
	}
	// 旧 web 进程必须真的消失。
	if pidAlive(oldPIDs["web"]) {
		t.Fatal("old web process survived update (process accumulation)")
	}
	t.Log("judgement: update removed web (process reaped), added cache, generation=2, no port/process accumulation")
	_ = h.engine.Delete(context.Background(), "upd")
}

// 更新后旧 PID 文件不得残留（受控重建清理干净）。
func TestUpdateNoStalePidFiles(t *testing.T) {
	h := newHarness(t, 4, 41000, 41080, enginePolicyReject())
	env := h.apply(basicSpec("pidf"))
	_ = env
	spec2 := basicSpec("pidf")
	spec2.Services = spec2.Services[:2] // 去掉 web
	_, _ = h.engine.Apply(context.Background(), spec2)
	entries := listPidDir(t, h.dir, "pidf")
	if len(entries) != 2 {
		t.Fatalf("want 2 pid files after removing web, got %v", entries)
	}
	t.Logf("judgement: pid files after shrink = %v (no stale web.pid)", entries)
	_ = h.engine.Delete(context.Background(), "pidf")
}

// 多个不同环境并发调谐/对账互不干扰，各自资源独立、总数正确。
func TestMultipleEnvironmentsConcurrentReconcile(t *testing.T) {
	h := newHarness(t, 16, 42000, 42200, enginePolicyReject())
	const n = 8
	for i := 0; i < n; i++ {
		h.apply(basicSpec(envName(i)))
	}
	if h.engine.QuotaInUse() != n {
		t.Fatalf("quota=%d, want %d", h.engine.QuotaInUse(), n)
	}
	// 并发对账所有环境，同时并发 Get。
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			for i := 0; i < n; i++ {
				env, ok := h.engine.Get(envName(i))
				if !ok || env.Phase != domain.PhaseReady {
					t.Errorf("env %s unstable during concurrent reconcile", envName(i))
				}
			}
		}
	}()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := h.engine.Reconcile(context.Background(), envName(i)); err != nil {
				t.Errorf("reconcile %s: %v", envName(i), err)
			}
		}(i)
	}
	wg.Wait()
	close(done)
	if h.engine.QuotaInUse() != n || h.engine.PortsInUse() != 2*n {
		t.Fatalf("after concurrent reconcile quota=%d ports=%d, want %d/%d",
			h.engine.QuotaInUse(), h.engine.PortsInUse(), n, 2*n)
	}
	t.Logf("judgement: %d independent environments concurrent reconcile stable: quota=%d ports=%d", n, n, 2*n)
	for i := 0; i < n; i++ {
		_ = h.engine.Delete(context.Background(), envName(i))
	}
}
