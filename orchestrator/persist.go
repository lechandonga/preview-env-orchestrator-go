package orchestrator

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/lechandonga/preview-env-orchestrator-go/spec"
)

// 状态阶段：用于识别崩溃在"创建一半"或"销毁一半"的环境。
const (
	phaseCreating   = "creating"
	phaseActive     = "active"
	phaseDestroying = "destroying"
)

const (
	portsFileName  = "ports.json"
	ledgerFileName = "ledger.log"
	lockFileName   = "engine.lock"
	stateVersion   = 1
)

// boundSecretRef 是落盘的密钥绑定：只有 key@version 引用，绝不包含值。
type boundSecretRef struct {
	Key     string `json:"key"`
	Version int    `json:"version"`
}

type svcRecord struct {
	Name    string                    `json:"name"`
	SvcKey  string                    `json:"svc_key"`
	PID     int                       `json:"pid"` // 0 = 无运行进程
	Ports   map[string]int            `json:"ports,omitempty"`
	Secrets map[string]boundSecretRef `json:"secrets,omitempty"` // 环境变量名 -> 引用
	Blocked bool                      `json:"blocked,omitempty"`
	Failed  bool                      `json:"failed,omitempty"`
}

// envRecord 是环境的落盘形态。字段中不允许出现任何密钥值。
type envRecord struct {
	Version    int          `json:"version"`
	Name       string       `json:"name"`
	Spec       spec.EnvSpec `json:"spec"`
	SpecKey    string       `json:"spec_key"`
	Phase      string       `json:"phase"`
	Status     Status       `json:"status"`
	CreatedAt  time.Time    `json:"created_at"`
	ExpiresAt  time.Time    `json:"expires_at,omitempty"`
	Services   []svcRecord  `json:"services"`
	CleanupErr string       `json:"cleanup_err,omitempty"`
}

func envStatePath(dir, name string) string {
	return filepath.Join(dir, "env-"+name+".json")
}

// saveRecord 原子写入环境记录（temp + rename）。
func saveRecord(dir string, r *envRecord) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	path := envStatePath(dir, r.Name)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func deleteRecord(dir, name string) error {
	err := os.Remove(envStatePath(dir, name))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// loadRecords 读取全部环境记录，按名称排序保证跨实例行为确定。
func loadRecords(dir string) ([]*envRecord, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []*envRecord
	for _, ent := range entries {
		name := ent.Name()
		if len(name) < 5 || name[:4] != "env-" || filepath.Ext(name) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		var r envRecord
		if err := json.Unmarshal(data, &r); err != nil {
			return nil, fmt.Errorf("decode %s: %w", name, err)
		}
		out = append(out, &r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// fileLock 封装跨进程 flock。同一进程内的互斥仍由 Orchestrator.mu 保证。
type fileLock struct {
	f *os.File
}

func newFileLock(path string) (*fileLock, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	return &fileLock{f: f}, nil
}

func (l *fileLock) lock(exclusive bool) error {
	flag := syscall.LOCK_EX
	if !exclusive {
		flag = syscall.LOCK_SH
	}
	return syscall.Flock(int(l.f.Fd()), flag)
}

func (l *fileLock) unlock() error { return syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN) }

func (l *fileLock) close() {
	l.unlock()
	l.f.Close()
}

// appendLedgerFile 以 O_APPEND 追加一条流水 JSON（调用方持有跨进程锁）。
func appendLedgerFile(path string, e Event) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(data, '\n'))
	return err
}

// loadLedgerFile 载入共享流水历史（新启动的引擎实例据此看到此前全部流水）。
func loadLedgerFile(path string) []Event {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	var events []Event
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		var e Event
		if err := json.Unmarshal(sc.Bytes(), &e); err == nil {
			events = append(events, e)
		}
	}
	return events
}
