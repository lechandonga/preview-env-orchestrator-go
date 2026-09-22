package orchestrator

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lechandonga/preview-env-orchestrator-go/spec"
)

// TestConcurrentApplySameSpec 并发提交同一描述：只能得到一个实例，副作用不重复。
func TestConcurrentApplySameSpec(t *testing.T) {
	o, _ := newTestOrch(t, func(c *Config) { c.MaxEnvs = 64 })
	mkSpec := func() *spec.EnvSpec {
		x := svc("web")
		x.Ports = []spec.PortRequest{{Name: "http"}}
		return &spec.EnvSpec{Name: "shared", Services: []spec.ServiceSpec{x, svc("worker", "web")}}
	}
	const n = 32
	keys := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			env, err := o.Apply(context.Background(), mkSpec())
			if err != nil {
				t.Errorf("apply: %v", err)
				return
			}
			keys[i] = env.SpecKey
		}(i)
	}
	wg.Wait()
	for i := 1; i < n; i++ {
		if keys[i] != keys[0] {
			t.Fatalf("并发提交得到不同实例: %s vs %s", keys[i], keys[0])
		}
	}
	if got := o.Ledger().Count("alloc-port"); got != 1 {
		t.Fatalf("端口只能分配一次, got %d", got)
	}
	if got := o.Ledger().Count("start-proc"); got != 2 {
		t.Fatalf("进程只能启动一次/服务, got %d", got)
	}
	t.Logf("判定依据: %d 个并发 Apply 得到同一 SpecKey=%s, alloc-port=1, start-proc=2", n, keys[0])
	dumpLedger(t, o)
}

// TestConcurrentDestroyAndGet 并发销毁与访问：不得半创建、重复释放，访问方拿不到销毁中的实例。
func TestConcurrentDestroyAndGet(t *testing.T) {
	o, _ := newTestOrch(t, nil)
	x := svc("web")
	x.Ports = []spec.PortRequest{{Name: "http"}}
	s := &spec.EnvSpec{Name: "victim", Services: []spec.ServiceSpec{x}}
	if _, err := o.Apply(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	alloc := o.Ledger().Count("alloc-port")

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := o.Destroy("victim")
			if err != nil && !errors.Is(err, ErrNotFound) {
				t.Errorf("destroy: %v", err)
			}
		}()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				env, err := o.Get("victim")
				if err != nil {
					if !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrDestroying) {
						t.Errorf("get: unexpected %v", err)
					}
					return
				}
				if env.Status == StatusDestroying || env.Status == StatusDestroyed {
					t.Errorf("访问方拿到销毁中的实例: %s", env.Status)
					return
				}
			}
		}()
	}
	wg.Wait()
	if _, err := o.Get("victim"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("销毁后应不可见, got %v", err)
	}
	if n := o.Ports().InUse(); n != 0 {
		t.Fatalf("端口残留: %d", n)
	}
	if got := o.Ledger().Count("release-port"); got != alloc {
		t.Fatalf("重复或遗漏释放: alloc=%d release=%d", alloc, got)
	}
	if got := o.Ledger().Count("destroy"); got != 1 {
		t.Fatalf("销毁只能生效一次, got %d", got)
	}
	t.Logf("判定依据: alloc=%d release=%d, destroy 事件=1, 无端口残留, 访问方未见销毁中实例", alloc, o.Ledger().Count("release-port"))
	dumpLedger(t, o)
}

// TestExpiryRace 到期回收与并发销毁/访问竞争：只回收一次，不误删未到期环境。
func TestExpiryRace(t *testing.T) {
	o, _ := newTestOrch(t, func(c *Config) { c.MaxEnvs = 8 })
	x := svc("web")
	x.Ports = []spec.PortRequest{{Name: "http"}}
	expired := &spec.EnvSpec{Name: "exp", TTL: time.Millisecond, Services: []spec.ServiceSpec{x}}
	if _, err := o.Apply(context.Background(), expired); err != nil {
		t.Fatal(err)
	}
	keep := &spec.EnvSpec{Name: "keep", Services: []spec.ServiceSpec{svc("web")}}
	if _, err := o.Apply(context.Background(), keep); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); o.ReapExpired(time.Now()) }()
		wg.Add(1)
		go func() { defer wg.Done(); o.Destroy("exp") }()
		wg.Add(1)
		go func() { defer wg.Done(); o.Get("exp") }()
	}
	wg.Wait()

	if _, err := o.Get("exp"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("到期环境应被回收, got %v", err)
	}
	if _, err := o.Get("keep"); err != nil {
		t.Fatalf("未到期环境不得被误删: %v", err)
	}
	if got := o.Ledger().Count("destroy"); got != 1 {
		t.Fatalf("到期环境只能被回收一次, destroy=%d", got)
	}
	t.Logf("判定依据: exp 被回收且 destroy 事件=1, keep 仍在, 端口占用=%d", o.Ports().InUse())
	dumpLedger(t, o)
}

// TestReapRetryAfterFailure 回收失败可重试，且重试不重复释放。
func TestReapRetryAfterFailure(t *testing.T) {
	o, _ := newTestOrch(t, nil)
	x := svc("web")
	x.Ports = []spec.PortRequest{{Name: "http"}}
	s := &spec.EnvSpec{Name: "doomed", TTL: time.Millisecond, Services: []spec.ServiceSpec{x}}
	if _, err := o.Apply(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)

	fail := true
	o.cleanupHook = func(string) error {
		if fail {
			return errors.New("injected cleanup failure")
		}
		return nil
	}
	o.ReapExpired(time.Now())
	if got := o.List(); len(got) != 1 {
		t.Fatalf("回收失败应保留环境待重试: %v", got)
	}
	if got := o.Ledger().Count("reap-failed"); got != 1 {
		t.Fatalf("应记录回收失败, got %d", got)
	}
	released := o.Ledger().Count("release-port")
	fail = false
	o.ReapExpired(time.Now())
	if got := o.List(); len(got) != 0 {
		t.Fatalf("重试应成功: %v", got)
	}
	if got := o.Ledger().Count("release-port"); got != released {
		t.Fatalf("重试不得重复释放: before=%d after=%d", released, got)
	}
	t.Logf("判定依据: 首次回收注入失败 -> 环境保留; 重试成功; release-port 前后均为 %d（无重复释放）", released)
	dumpLedger(t, o)
}

// TestRollbackFailureAndReconcileRetry 回滚/清理失败可区分、可重试、不留孤儿资源。
func TestRollbackFailureAndReconcileRetry(t *testing.T) {
	o, _ := newTestOrch(t, nil)
	x := svc("web")
	x.Ports = []spec.PortRequest{{Name: "http"}}
	s := &spec.EnvSpec{Name: "rb", Services: []spec.ServiceSpec{x}}
	if _, err := o.Apply(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	o.cleanupHook = func(string) error { return errors.New("injected cleanup failure") }

	err := o.Destroy("rb")
	if !errors.Is(err, ErrRollbackFailed) {
		t.Fatalf("清理失败必须可区分, got %v", err)
	}
	if got := o.List(); len(got) != 1 {
		t.Fatalf("失败的环境应保留待对账: %v", got)
	}
	env, _ := o.Get("rb")
	if env.Status != StatusFailed {
		t.Fatalf("状态应为 Failed, got %s", env.Status)
	}
	// 端口已在失败的回收中释放，重试不得重复释放。
	released := o.Ledger().Count("release-port")
	if n := o.Ports().InUse(); n != 0 {
		t.Fatalf("端口应已释放: %d", n)
	}

	o.cleanupHook = nil
	if err := o.Reconcile("rb"); err != nil {
		t.Fatalf("对账重试应成功: %v", err)
	}
	if _, err := o.Get("rb"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("对账后环境应被清理, got %v", err)
	}
	if got := o.Ledger().Count("release-port"); got != released {
		t.Fatalf("重试不得重复释放端口: before=%d after=%d", released, got)
	}
	// 槽位已释放，可再建环境。
	if _, err := o.Apply(context.Background(), &spec.EnvSpec{Name: "rb2", Services: []spec.ServiceSpec{svc("web")}}); err != nil {
		t.Fatalf("槽位应被释放: %v", err)
	}
	t.Logf("判定依据: Destroy -> ErrRollbackFailed, 环境保留为 Failed; Reconcile 重试成功; release-port 恒为 %d", released)
	dumpLedger(t, o)
}

// TestReconcileDrift 外部改动导致偏离：对账修复（重启进程、重分配端口）。
func TestReconcileDrift(t *testing.T) {
	o, _ := newTestOrch(t, nil)
	x := svc("web")
	x.Ports = []spec.PortRequest{{Name: "http"}}
	s := &spec.EnvSpec{Name: "drift", Services: []spec.ServiceSpec{x}}
	env, err := o.Apply(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	port := env.Services["web"].Ports["http"]

	// 外部杀死进程 + 外部释放端口（模拟漂移）。
	o.mu.Lock()
	pid := o.envs["drift"].svcs["web"].proc.PID()
	o.mu.Unlock()
	syscall.Kill(pid, syscall.SIGKILL)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		o.mu.Lock()
		alive := o.envs["drift"].svcs["web"].proc.Alive()
		o.mu.Unlock()
		if !alive {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	o.ports.Release("drift", port)

	if err := o.Reconcile("drift"); err != nil {
		t.Fatalf("对账应修复漂移: %v", err)
	}
	env2, err := o.Get("drift")
	if err != nil {
		t.Fatal(err)
	}
	if !env2.Services["web"].Running {
		t.Fatal("进程应被对账重启")
	}
	o.mu.Lock()
	newPID := o.envs["drift"].svcs["web"].proc.PID()
	o.mu.Unlock()
	if newPID == pid {
		t.Fatalf("应是新进程: old=%d new=%d", pid, newPID)
	}
	newPort := env2.Services["web"].Ports["http"]
	if owner, ok := o.Ports().Owner(newPort); !ok || owner != "drift" {
		t.Fatalf("端口应重新归属环境: port=%d owner=%s", newPort, owner)
	}
	t.Logf("判定依据: 进程 pid %d->%d 被重启, 端口 %d->%d 重新归属 drift", pid, newPID, port, newPort)
	dumpLedger(t, o)
}

// TestConcurrentReconcile 并发调谐与销毁：竞态检测下状态一致。
func TestConcurrentReconcile(t *testing.T) {
	o, _ := newTestOrch(t, nil)
	s := &spec.EnvSpec{Name: "rc", Services: []spec.ServiceSpec{svc("a"), svc("b", "a")}}
	if _, err := o.Apply(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if err := o.Reconcile("rc"); err != nil && !errors.Is(err, ErrDestroying) && !errors.Is(err, ErrNotFound) {
					t.Errorf("reconcile: %v", err)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		o.Destroy("rc")
	}()
	wg.Wait()
	if _, err := o.Get("rc"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("最终应被销毁, got %v", err)
	}
	if n := o.Ports().InUse(); n != 0 {
		t.Fatalf("无端口残留, got %d", n)
	}
	t.Log("判定依据: 并发调谐+销毁后环境不存在、端口零残留")
}
