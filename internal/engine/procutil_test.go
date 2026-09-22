package engine_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func newSleepCmd() *exec.Cmd { return exec.Command("sleep", "30") }

func nowMillis() int64 { return time.Now().UnixMilli() }
func sleepMs(ms int)   { time.Sleep(time.Duration(ms) * time.Millisecond) }

func envName(i int) string { return "multi-" + strconv.Itoa(i) }

func pidAlive(pid int) bool { return syscall.Kill(pid, 0) == nil }

func listPidDir(t *testing.T, stateDir, env string) []string {
	t.Helper()
	dir := filepath.Join(stateDir, "sandbox", env)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read sandbox dir: %v", err)
	}
	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".pid") {
			names = append(names, e.Name())
		}
	}
	return names
}

var _ = sync.Mutex{}
