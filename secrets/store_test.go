package secrets

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestVersioning(t *testing.T) {
	s := NewStore()
	v1, err := s.Put("k", "a")
	if err != nil || v1 != 1 {
		t.Fatalf("v1=%d err=%v", v1, err)
	}
	if _, err := s.Put("k", "x"); !errors.Is(err, ErrExists) {
		t.Fatalf("重复 Put 应拒绝: %v", err)
	}
	v2, _ := s.Rotate("k", "b")
	if v2 != 2 {
		t.Fatalf("v2=%d", v2)
	}
	if got, _ := s.Get("k", 1); got != "a" {
		t.Fatalf("旧版本应仍可读, got %q", got)
	}
	if got, _ := s.Get("k", 0); got != "b" {
		t.Fatalf("version=0 应读最新, got %q", got)
	}
	if _, err := s.Get("k", 9); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("失效版本应明确失败: %v", err)
	}
	if _, err := s.Get("ghost", 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("缺失密钥应明确失败: %v", err)
	}
	t.Log("判定依据: 版本固定可读、缺失/失效版本均返回可区分错误，无默认值退化")
}

func TestConcurrentAccess(t *testing.T) {
	s := NewStore()
	s.Put("k", "v1")
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(2)
		go func(i int) { defer wg.Done(); s.Rotate("k", fmt.Sprint(i)) }(i)
		go func() { defer wg.Done(); s.Get("k", 0) }()
	}
	wg.Wait()
	latest, _ := s.Latest("k")
	if latest != 33 {
		t.Fatalf("latest=%d, want 33", latest)
	}
}

func TestRevoke(t *testing.T) {
	s := NewStore()
	s.Put("k", "a")
	if v, _ := s.Rotate("k", "b"); v != 2 {
		t.Fatalf("v2=%d", v)
	}
	if err := s.Revoke("k", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get("k", 1); !errors.Is(err, ErrRevoked) {
		t.Fatalf("吊销版本必须 ErrRevoked, got %v", err)
	}
	if got, err := s.Get("k", 2); err != nil || got != "b" {
		t.Fatalf("未吊销版本应可读, got %q err=%v", got, err)
	}
	// 全量吊销后最新版本也不可读，绝不退化。
	if err := s.Revoke("k", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get("k", 0); !errors.Is(err, ErrRevoked) {
		t.Fatalf("全量吊销后最新版本也必须失败, got %v", err)
	}
	if err := s.Revoke("ghost", 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("吊销缺失密钥应 ErrNotFound, got %v", err)
	}
	t.Log("判定依据: 单版本/全量吊销后 Get 明确 ErrRevoked，未吊销版本不受影响")
}
