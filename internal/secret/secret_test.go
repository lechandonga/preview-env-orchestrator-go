package secret_test

import (
	"errors"
	"testing"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/secret"
)

func TestRotateAndPinnedVersion(t *testing.T) {
	s := secret.NewStore()
	v1, err := s.Put("db-password", "old-value")
	if err != nil {
		t.Fatal(err)
	}
	if v1 != 1 {
		t.Fatalf("first version should be 1, got %d", v1)
	}
	v2, err := s.Rotate("db-password", "new-value")
	if err != nil {
		t.Fatal(err)
	}

	// 未固定版本：取当前（最新）。
	got, err := s.Resolve("db-password", 0)
	if err != nil || got != "new-value" {
		t.Fatalf("current should resolve to new value, got %q err=%v", got, err)
	}
	// 已绑定旧版本：继续读到旧值。
	got, err = s.Resolve("db-password", v1)
	if err != nil || got != "old-value" {
		t.Fatalf("pinned v1 must keep old value, got %q err=%v", got, err)
	}
	_ = v2
	t.Log("judgement: rotation moves floating refs to v2 while pinned v1 keeps old value")
}

func TestSecretMissingAndDisabled(t *testing.T) {
	s := secret.NewStore()
	if _, err := s.Resolve("nope", 0); !errors.Is(err, secret.ErrNotFound) {
		t.Fatalf("missing secret must error, got %v", err)
	}
	v, _ := s.Put("k", "val")
	if err := s.DisableVersion("k", v); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Resolve("k", v); !errors.Is(err, secret.ErrVersionDisabled) {
		t.Fatalf("disabled version must error, got %v", err)
	}
	if _, err := s.Resolve("k", 99); !errors.Is(err, secret.ErrVersionNotFound) {
		t.Fatalf("invalid version must error, got %v", err)
	}
	t.Log("judgement: missing/disabled/invalid-version references all fail explicitly, never a default")
}
