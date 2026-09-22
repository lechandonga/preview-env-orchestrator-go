package ports

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestAllocatePreferredAndRealloc(t *testing.T) {
	a := New(10000, 10002)
	p1, err := a.Allocate("e1", 10000)
	if err != nil || p1 != 10000 {
		t.Fatalf("p1=%d err=%v", p1, err)
	}
	p2, err := a.Allocate("e2", 10000) // 冲突 -> 重分配
	if err != nil || p2 == 10000 {
		t.Fatalf("冲突应重分配: p2=%d err=%v", p2, err)
	}
	t.Logf("判定依据: 首选 %d 归 e1, e2 冲突后重分配到 %d", p1, p2)
}

func TestExhaustion(t *testing.T) {
	a := New(10000, 10001)
	a.Allocate("e1", 0)
	a.Allocate("e1", 0)
	if _, err := a.Allocate("e2", 0); !errors.Is(err, ErrExhausted) {
		t.Fatalf("got %v", err)
	}
}

func TestReleaseOwnerOnly(t *testing.T) {
	a := New(10000, 10010)
	p, _ := a.Allocate("e1", 0)
	a.Release("e2", p) // 非持有者不得释放
	if a.InUse() != 1 {
		t.Fatal("非持有者释放不应生效")
	}
	a.Release("e1", p)
	a.Release("e1", p) // 幂等
	if a.InUse() != 0 {
		t.Fatal("释放后应为 0")
	}
}

func TestConcurrentAllocate(t *testing.T) {
	a := New(20000, 20063)
	var wg sync.WaitGroup
	var mu sync.Mutex
	got := map[int]bool{}
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p, err := a.Allocate(fmt.Sprint(i), 0)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			if got[p] {
				t.Errorf("端口 %d 被重复分配", p)
			}
			got[p] = true
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if a.InUse() != 64 {
		t.Fatalf("InUse=%d", a.InUse())
	}
}
