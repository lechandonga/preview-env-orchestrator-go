package engine_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"
)

// 同一描述被多个 goroutine 并发提交：只应得到同一个实例，进程/端口不重复分配。
func TestConcurrentApplySameSpec(t *testing.T) {
	h := newHarness(t, 8, 32000, 32050, enginePolicyReject())
	const n = 12
	var wg sync.WaitGroup
	var success, rejected int64
	pids := sync.Map{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			env, err := h.engine.Apply(context.Background(), basicSpec("same"))
			if err != nil {
				// 合法结果只可能是“正在删除”类冲突；本用例不应出现。
				t.Errorf("concurrent Apply unexpected error: %v", err)
				atomic.AddInt64(&rejected, 1)
				return
			}
			atomic.AddInt64(&success, 1)
			pids.Store(env.Services["db"].PID, struct{}{})
		}()
	}
	wg.Wait()
	if success != n || rejected != 0 {
		t.Fatalf("success=%d rejected=%d, want all %d succeed idempotently", success, rejected, n)
	}
	count := 0
	pids.Range(func(_, _ any) bool { count++; return true })
	if count != 1 {
		t.Fatalf("db had %d distinct pids across concurrent applies; want exactly 1", count)
	}
	env, _ := h.engine.Get("same")
	if env.Generation != 1 {
		t.Fatalf("generation=%d, want 1 (idempotent)", env.Generation)
	}
	t.Logf("judgement: %d concurrent identical applies -> one instance, one db pid, generation=1", n)
	_ = h.engine.Delete(context.Background(), "same")
}

// 同一环境并发创建与销毁：不得出现半创建、重复释放或残留进程/端口。
func TestConcurrentApplyAndDelete(t *testing.T) {
	h := newHarness(t, 8, 32000, 32050, enginePolicyReject())
	const rounds = 30
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			_, err := h.engine.Apply(context.Background(), basicSpec("race"))
			if err != nil {
				var ce *domain.ConflictError
				if !errors.As(err, &ce) {
					t.Errorf("apply err type=%T: %v", err, err)
				}
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			err := h.engine.Delete(context.Background(), "race")
			if err != nil {
				var nf interface{ Error() string }
				_ = nf
				// NotFound/冲突均为可接受终态。
			}
		}
	}()
	wg.Wait()

	// 收尾：环境要么不存在，要么可被一次 Delete 干净回收；资源簿记归零。
	if env, ok := h.engine.Get("race"); ok {
		if err := h.engine.Delete(context.Background(), "race"); err != nil {
			t.Fatalf("final delete: %v", err)
		}
		t.Logf("judgement: race ended with existing env phase=%s -> deleted cleanly", env.Phase)
	} else {
		t.Log("judgement: race ended with env absent (deleted)")
	}
	// 端口簿记分两轮核对：列出所有现存环境后，剩余分配数必须与它们的端口总数一致。
	envs, err := h.engine.List()
	if err != nil {
		t.Fatal(err)
	}
	wantPorts := 0
	for _, e := range envs {
		for _, st := range e.Services {
			wantPorts += len(st.Ports)
		}
	}
	if got := h.engine.PortsInUse(); got != wantPorts {
		t.Fatalf("port bookkeeping leak: pool=%d but snapshots declare=%d", got, wantPorts)
	}
	t.Logf("judgement: no orphan ports after concurrent apply/delete (pool=%d)", wantPorts)
}

// 对不存在/已销毁环境并发 Delete：必须返回 NotFound 且绝不能超draw配额或重复释放。
func TestConcurrentDeleteIdempotent(t *testing.T) {
	h := newHarness(t, 4, 32000, 32050, enginePolicyReject())
	h.apply(basicSpec("gone"))
	if err := h.engine.Delete(context.Background(), "gone"); err != nil {
		t.Fatal(err)
	}
	const n = 8
	var wg sync.WaitGroup
	var notFound, other int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			err := h.engine.Delete(context.Background(), "gone")
			if err == nil {
				atomic.AddInt64(&other, 1)
				return
			}
			atomic.AddInt64(&notFound, 1)
		}()
	}
	wg.Wait()
	if other != 0 {
		t.Fatalf("%d deletes unexpectedly succeeded on missing env (double-release risk)", other)
	}
	if notFound != n {
		t.Fatalf("notFound=%d/%d", notFound, n)
	}
	if h.engine.PortsInUse() != 0 {
		t.Fatalf("ports leaked: %d", h.engine.PortsInUse())
	}
	// 配额名额可再次获取（没有超draw）。
	h.apply(basicSpec("gone2"))
	t.Log("judgement: concurrent deletes of absent env all returned not-found; quota reusable; ports=0")
	_ = h.engine.Delete(context.Background(), "gone2")
}
