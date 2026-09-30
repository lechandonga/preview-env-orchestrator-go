// Package state 提供多引擎实例共享一份状态所需的磁盘设施：
// 跨进程互斥锁（flock）、原子文件写入（tmp+rename+fsync）、
// 实例心跳注册与共享事件流水。本包不感知编排语义。
package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// Store 是一份共享状态目录的句柄。
type Store struct {
	dir string

	mu     sync.Mutex
	lockFd *os.File // 全局跨进程锁
}

// Open 打开（必要时创建）共享状态目录。
func Open(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("state: empty dir")
	}
	for _, sub := range []string{"", "envs", "inst"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, fmt.Errorf("state: mkdir %s: %w", sub, err)
		}
	}
	f, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("state: open lock: %w", err)
	}
	return &Store{dir: dir, lockFd: f}, nil
}

// Dir 返回状态目录。
func (s *Store) Dir() string { return s.dir }

// Lock 阻塞获取全局排他锁（跨进程），返回释放函数。
// 同进程内仍需自行用互斥锁序列化（flock 关联打开文件表项）。
func (s *Store) Lock() func() {
	if err := syscall.Flock(int(s.lockFd.Fd()), syscall.LOCK_EX); err != nil {
		// 锁文件在 Open 时已固定，出错只可能是严重故障。
		panic(fmt.Sprintf("state: flock: %v", err))
	}
	return func() { syscall.Flock(int(s.lockFd.Fd()), syscall.LOCK_UN) }
}

// WriteJSON 原子写入 JSON：同目录临时文件 -> fsync -> rename，
// 保证任何时刻读到的都是完整记录，绝不出现半截 JSON。
func (s *Store) WriteJSON(rel string, v any) error {
	dst := filepath.Join(s.dir, rel)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if err := json.NewEncoder(tmp).Encode(v); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, dst); err != nil {
		os.Remove(tmpName)
		return err
	}
	// fsync 目录，保证 rename 落盘（强杀/掉电后仍可恢复）。
	if d, err := os.Open(filepath.Dir(dst)); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// ReadJSON 读取并反序列化；文件不存在返回 ok=false。
func (s *Store) ReadJSON(rel string, v any) (bool, error) {
	b, err := os.ReadFile(filepath.Join(s.dir, rel))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return false, err
	}
	return true, nil
}

// Remove 删除一条状态文件（删除本身在全局锁保护下即为幂等判定点）。
func (s *Store) Remove(rel string) error {
	err := os.Remove(filepath.Join(s.dir, rel))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// List 列出某子目录下文件名（不含目录前缀）。
func (s *Store) List(sub string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(s.dir, sub))
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	return names, nil
}

// AppendJournal 在全局事件流水末尾追加一行 JSON（调用方须持有全局锁）。
func (s *Store) AppendJournal(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	f, err := os.OpenFile(filepath.Join(s.dir, "events.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		return err
	}
	return f.Sync()
}

// ReadJournal 读回全部共享流水行（测试做全局判定用）。
func (s *Store) ReadJournal() ([][]byte, error) {
	b, err := os.ReadFile(filepath.Join(s.dir, "events.log"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out [][]byte
	start := 0
	for i := 0; i < len(b); i++ {
		if b[i] == '\n' {
			out = append(out, b[start:i])
			start = i + 1
		}
	}
	return out, nil
}
