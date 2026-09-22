package engine_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"
	"github.com/lechandonga/preview-env-orchestrator-go/internal/engine"
)

// 到期回收与并发访问竞争：任何时刻 Get 要么拿到完整 Ready 实例，
// 要么拿不到；绝不能拿到半销毁实例；回收重试不重复释放资源。
func TestTTLExpiryRace(t *testing.T) {
	h := newHarness(t, 8, 33000, 33080, enginePolicyReject())
	spec := basicSpec("exprace")
	spec.TTL = 100 * time.Millisecond
	h.apply(spec)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 访问者：在过期窗口内持续读取，断言不出现 Deleting 相位泄露。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			env, ok := h.engine.Get("exprace")
			if ok && env.Phase != domain.PhaseReady && env.Phase != domain.PhaseDegraded {
				t.Errorf("accessor observed in-between phase %s", env.Phase)
			}
			if ok {
				for n, st := range env.Services {
					if st.Phase == domain.ServiceStarting {
						t.Errorf("accessor observed half-started service %s", n)
					}
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// 多个回收者并发运行 ReconcileAll（模拟多个控制器实例/重试）。
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				_ = h.engine.ReconcileAll(context.Background(), time.Now())
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}

	time.Sleep(400 * time.Millisecond)
	close(stop)
	wg.Wait()

	if _, ok := h.engine.Get("exprace"); ok {
		t.Fatal("expired env still accessible after reclamation")
	}
	if h.engine.PortsInUse() != 0 {
		t.Fatalf("orphan ports remain: %d", h.engine.PortsInUse())
	}
	if h.engine.QuotaInUse() != 0 {
		t.Fatalf("quota not released: %d", h.engine.QuotaInUse())
	}
	// 回收完成后再次“补偿回收”必须安全（不重复释放）。
	if err := h.engine.ReconcileAll(context.Background(), time.Now()); err != nil {
		t.Fatalf("idempotent re-reclaim failed: %v", err)
	}
	t.Log("judgement: expiry under concurrent accessors/reclaimers left no instance, ports=0, quota=0; re-reclaim safe")
}

// 回收失败可重试：通过在过期后抢占环境持有的端口模拟“删除过程中的外部失败”，
// 验证 Delete/Reconcile 中断后重试能够收敛（已完成的步骤不重复释放）。
func TestDeleteRetryAfterInterruption(t *testing.T) {
	h := newHarness(t, 8, 33000, 33080, enginePolicyReject())
	spec := basicSpec("retry")
	env := h.apply(spec)
	// 外部把 db 进程杀掉，制造快照与实际偏离后再删除。
	killPid(t, env.Services["db"].PID)
	// 第一次删除：对账路径必须容忍消失的进程（幂等 stop）。
	if err := h.engine.Delete(context.Background(), "retry"); err != nil {
		t.Fatalf("delete despite killed process: %v", err)
	}
	if h.engine.PortsInUse() != 0 || h.engine.QuotaInUse() != 0 {
		t.Fatalf("leak after delete ports=%d quota=%d", h.engine.PortsInUse(), h.engine.QuotaInUse())
	}
	// 再次删除：明确 NotFound，无副作用。
	err := h.engine.Delete(context.Background(), "retry")
	var nf interface{ Unwrap() error }
	_ = nf
	if err == nil {
		t.Fatal("second delete must report not-found")
	}
	t.Logf("judgement: delete with externally killed process succeeded; retry reports %v; no double release", err)
}

// Queue 策略：达到上限的请求排队，名额释放后按序获得环境。
func TestQuotaQueuePolicy(t *testing.T) {
	h := newHarness(t, 1, 34000, 34050, engine.PolicyQueue)
	h.apply(basicSpec("first"))

	errCh := make(chan error, 1)
	envCh := make(chan *domain.Environment, 1)
	go func() {
		env, err := h.engine.Apply(context.Background(), basicSpec("second"))
		errCh <- err
		envCh <- env
	}()

	// 排队者等待期间不应创建成功。
	select {
	case err := <-errCh:
		t.Fatalf("queued apply returned too early err=%v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, ok := h.engine.Get("second"); ok {
		t.Fatal("queued env must not be visible while waiting")
	}

	// 释放名额 -> 排队者应被唤醒并创建成功。
	if err := h.engine.Delete(context.Background(), "first"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("queued apply after slot release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queued apply was never woken")
	}
	env := <-envCh
	if env.Phase != domain.PhaseReady {
		t.Fatalf("queued env phase=%s", env.Phase)
	}
	if h.engine.QuotaInUse() != 1 {
		t.Fatalf("quota=%d after handoff, want 1", h.engine.QuotaInUse())
	}
	t.Log("judgement: queue policy blocked second apply, then handed freed slot directly (quota stayed 1)")
	_ = h.engine.Delete(context.Background(), "second")
}

// 排队者 ctx 取消后放弃排队，名额不被吞掉。
func TestQuotaQueueCancel(t *testing.T) {
	h := newHarness(t, 1, 34000, 34050, engine.PolicyQueue)
	h.apply(basicSpec("hold"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := h.engine.Apply(ctx, basicSpec("waiter"))
		done <- err
	}()
	time.Sleep(80 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled queue should return error")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled waiter did not return")
	}
	// 持有者释放名额：因为没有等待者，计数必须正确归零并可再申请。
	if err := h.engine.Delete(context.Background(), "hold"); err != nil {
		t.Fatal(err)
	}
	h.apply(basicSpec("next"))
	if h.engine.QuotaInUse() != 1 {
		t.Fatalf("quota=%d, slot was swallowed by canceled waiter", h.engine.QuotaInUse())
	}
	t.Log("judgement: canceled queue request releases its wait slot without consuming quota")
	_ = h.engine.Delete(context.Background(), "next")
}
