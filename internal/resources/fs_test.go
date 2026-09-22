package resources_test

import (
	"path/filepath"
	"testing"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/resources"
)

func TestFSQuotaCrossInstance(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "shared")
	fs1, err := resources.NewFSQuota(dir)
	if err != nil {
		t.Fatal(err)
	}
	fs2, err := resources.NewFSQuota(dir)
	if err != nil {
		t.Fatal(err)
	}
	ok, _ := fs1.TryAcquire(2)
	if !ok {
		t.Fatal("first acquire")
	}
	ok, _ = fs2.TryAcquire(2)
	if !ok {
		t.Fatal("second acquire")
	}
	ok, _ = fs1.TryAcquire(2)
	if ok {
		t.Fatal("third acquire across instances must be rejected at limit 2")
	}
	if err := fs2.Release(); err != nil {
		t.Fatal(err)
	}
	ok, _ = fs1.TryAcquire(2)
	if !ok {
		t.Fatal("after release by instance 2, instance 1 should acquire")
	}
	if fs1.InUse() != 2 || fs2.InUse() != 2 {
		t.Fatalf("cross-instance count inconsistent: %d vs %d", fs1.InUse(), fs2.InUse())
	}
	t.Log("judgement: two FSQuota handles on one dir share a single limit of 2")
}

func TestFSPortPoolCrossInstance(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "shared")
	p1, err := resources.NewFSPortPool(dir, 5000, 5002)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := resources.NewFSPortPool(dir, 5000, 5002)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := p1.Allocate(0)
	b, _ := p2.Allocate(0)
	c, _ := p1.Allocate(0)
	_ = c
	if a == b {
		t.Fatalf("two instances allocated same port %d", a)
	}
	if _, err := p2.Allocate(0); err == nil {
		t.Fatal("shared pool must exhaust across instances")
	}
	// p1 释放，p2 应能复用同一端口（位图权威在文件）。
	if err := p1.Release(a); err != nil {
		t.Fatal(err)
	}
	d, err := p2.Allocate(0)
	if err != nil || d != a {
		t.Fatalf("instance 2 should recycle port %d, got %d err=%v", a, d, err)
	}
	t.Logf("judgement: cross-instance dynamic allocations unique and recycled (ports=%d,%d,%d)", a, b, d)
}
