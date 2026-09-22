package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"
)

// ErrNotFound 表示目标环境快照不存在。
var ErrNotFound = errors.New("environment snapshot not found")

// Store 是按环境名索引的 JSON 文件存储，写入采用“临时文件 + rename”原子落盘，
// 保证崩溃后不会读到半写状态。
type Store struct {
	mu  sync.RWMutex
	dir string
}

// NewStore 在 dir 下创建/打开状态存储。
func NewStore(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("store dir must not be empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	return &Store{dir: dir}, nil
}

// Put 保存（创建或覆盖）环境快照。
func (s *Store) Put(env *domain.Environment) error {
	if env == nil || env.Name == "" {
		return errors.New("cannot persist unnamed environment")
	}
	data, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal environment %q: %w", env.Name, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	final := s.path(env.Name)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write temp snapshot: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("commit snapshot: %w", err)
	}
	return nil
}

// Get 读取环境快照；不存在时 ok=false（错误为 nil）。
func (s *Store) Get(name string) (*domain.Environment, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, err := os.ReadFile(s.path(name))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, err
	}
	var env domain.Environment
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, false, fmt.Errorf("corrupt snapshot %q: %w", name, err)
	}
	if env.Services == nil {
		env.Services = map[string]*domain.ServiceStatus{}
	}
	return &env, true, nil
}

// List 列出全部环境快照，按名称排序使结果确定。
func (s *Store) List() ([]*domain.Environment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var out []*domain.Environment
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") || strings.HasSuffix(e.Name(), ".tmp") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.dir, e.Name()))
		if err != nil {
			return nil, err
		}
		var env domain.Environment
		if err := json.Unmarshal(data, &env); err != nil {
			return nil, fmt.Errorf("corrupt snapshot %q: %w", e.Name(), err)
		}
		if env.Services == nil {
			env.Services = map[string]*domain.ServiceStatus{}
		}
		out = append(out, &env)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Delete 删除环境快照；删除不存在的环境返回包装了 ErrNotFound 的错误。
func (s *Store) Delete(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(s.path(name))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: %q", ErrNotFound, name)
		}
		return err
	}
	return nil
}

func (s *Store) path(name string) string {
	return filepath.Join(s.dir, safeName(name)+".json")
}

func safeName(s string) string {
	return strings.NewReplacer("/", "_", "..", "_", " ", "_").Replace(s)
}
