package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"
	"github.com/lechandonga/preview-env-orchestrator-go/internal/engine"
)

func enginePolicyReject() engine.LimitPolicy { return engine.PolicyReject }

func killPid(t *testing.T, pid int) {
	t.Helper()
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		t.Fatalf("kill %d: %v", pid, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); err != nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("pid %d did not exit after SIGTERM", pid)
}

func readFile(t *testing.T, dir, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, name+".json"))
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	return string(data)
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }

// --- 恢复测试使用的桥接辅助 ---

func tTempDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func setFlowLogger(t *testing.T, e *engine.Engine) {
	t.Helper()
	e.SetLogger(func(format string, args ...any) {
		t.Logf("[flow] "+format, args...)
	})
}

func applyWith(t *testing.T, e *engine.Engine, spec domain.EnvironmentSpec) *domain.Environment {
	t.Helper()
	env, err := e.Apply(context.Background(), spec)
	if err != nil {
		t.Fatalf("Apply(%s): %v", spec.Name, err)
	}
	return env
}

func snapshotGet(e *engine.Engine, name string) (*domain.Environment, bool, error) {
	return e.DebugLoadSnapshot(name)
}

func snapshotPersist(e *engine.Engine, env *domain.Environment) error {
	return e.DebugSaveSnapshot(env)
}

func nowTime() time.Time { return engine.DebugNow() }
