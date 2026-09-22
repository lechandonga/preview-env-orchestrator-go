package resources_test

import (
	"errors"
	"testing"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/resources"
)

func TestPortPoolAllocRelease(t *testing.T) {
	p := resources.NewPortPool(30000, 30002)
	p1, err := p.Allocate(0)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := p.Allocate(0)
	if err != nil {
		t.Fatal(err)
	}
	if p1 == p2 {
		t.Fatalf("allocated same port twice: %d", p1)
	}
	if _, err := p.Allocate(p1); !errors.Is(err, resources.ErrPortInUse) {
		t.Fatalf("expected ErrPortInUse, got %v", err)
	}
	if err := p.Release(p1); err != nil {
		t.Fatal(err)
	}
	p3, err := p.Allocate(0)
	if err != nil {
		t.Fatal(err)
	}
	if p3 != p1 {
		t.Fatalf("expected recycled port %d, got %d", p1, p3)
	}
	_ = p.Release(p2)
	_ = p.Release(p3)
	if err := p.Release(p3); !errors.Is(err, resources.ErrPortNotAllocated) {
		t.Fatalf("double release must fail, got %v", err)
	}
	t.Log("judgement: ports allocated uniquely, recycled, double-release rejected")
}

func TestPortPoolExhaust(t *testing.T) {
	p := resources.NewPortPool(40000, 40000)
	if _, err := p.Allocate(0); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Allocate(0); !errors.Is(err, resources.ErrNoPortAvailable) {
		t.Fatalf("expected exhaustion, got %v", err)
	}
	t.Log("judgement: exhausted pool returns ErrNoPortAvailable, no unbounded allocation")
}

func TestQuota(t *testing.T) {
	q := resources.NewQuota(1)
	if !q.TryAcquire() {
		t.Fatal("first acquire should succeed")
	}
	if q.TryAcquire() {
		t.Fatal("second acquire must fail at limit")
	}
	if err := q.Release(); err != nil {
		t.Fatal(err)
	}
	if err := q.Release(); !errors.Is(err, resources.ErrQuotaOverdraw) {
		t.Fatalf("overdraw must error, got %v", err)
	}
	t.Log("judgement: quota enforced at 1 and overdraw release rejected")
}
