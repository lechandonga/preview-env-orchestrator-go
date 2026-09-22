package store_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/store"
)

func TestFileLockExcludes(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	l1, err := store.LockDir(dir, "env-x")
	if err != nil {
		t.Fatal(err)
	}
	acquired := make(chan struct{})
	go func() {
		l2, err := store.LockDir(dir, "env-x")
		if err != nil {
			return
		}
		close(acquired)
		_ = l2.Unlock()
	}()
	select {
	case <-acquired:
		t.Fatal("second lock must not be acquired while first is held")
	case <-time.After(100 * time.Millisecond):
	}
	if err := l1.Unlock(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("second lock should be acquired after first unlock")
	}
	t.Log("judgement: flock provides mutual exclusion and is released on Unlock")
}

func TestFileLockDifferentNamesIndependent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	la, err := store.LockDir(dir, "a")
	if err != nil {
		t.Fatal(err)
	}
	lb, err := store.LockDir(dir, "b")
	if err != nil {
		t.Fatalf("different environments must not block each other: %v", err)
	}
	_ = la.Unlock()
	_ = lb.Unlock()
	t.Log("judgement: locks for different environments are independent (parallel envs)")
}
