package resources

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
)

// FSPortPool 是跨进程共享目录的独占端口池：用 flock 保护的位图 JSON 文件
// 记录已分配端口，使多个引擎实例共享同一端口范围时不会分到重复端口。
type FSPortPool struct {
	mu    sync.Mutex
	min   int
	max   int
	state string
	lock  string
}

// NewFSPortPool 创建/打开跨进程端口池，并在需要时初始化空位图。
func NewFSPortPool(dir string, minPort, maxPort int) (*FSPortPool, error) {
	if err := os.MkdirAll(filepath.Join(dir, "ports"), 0o755); err != nil {
		return nil, err
	}
	p := &FSPortPool{
		min:   minPort,
		max:   maxPort,
		state: filepath.Join(dir, "ports", "allocated.json"),
		lock:  filepath.Join(dir, "ports", "allocated.lock"),
	}
	if _, err := os.Stat(p.state); os.IsNotExist(err) {
		if err := p.write(map[int]bool{}); err != nil {
			return nil, err
		}
	}
	return p, nil
}

func (p *FSPortPool) withLock(fn func(in map[int]bool) error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	lf, err := os.OpenFile(p.lock, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lf.Close()
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lf.Fd()), syscall.LOCK_UN)

	in, err := p.read()
	if err != nil {
		return err
	}
	if err := fn(in); err != nil {
		return err
	}
	return p.write(in)
}

func (p *FSPortPool) read() (map[int]bool, error) {
	data, err := os.ReadFile(p.state)
	if err != nil {
		return nil, err
	}
	in := map[int]bool{}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &in); err != nil {
			return nil, err
		}
	}
	return in, nil
}

func (p *FSPortPool) write(in map[int]bool) error {
	data, err := json.Marshal(in)
	if err != nil {
		return err
	}
	tmp := p.state + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, p.state)
}

// Allocate 分配端口，语义与 PortPool 一致（preferred 强制/冲突错误；0 自动选取）。
func (p *FSPortPool) Allocate(preferred int) (int, error) {
	result := 0
	err := p.withLock(func(in map[int]bool) error {
		if preferred != 0 {
			if preferred < p.min || preferred > p.max {
				return ErrPortOutOfRange
			}
			if in[preferred] {
				return ErrPortInUse
			}
			in[preferred] = true
			result = preferred
			return nil
		}
		for port := p.min; port <= p.max; port++ {
			if !in[port] {
				in[port] = true
				result = port
				return nil
			}
		}
		return ErrNoPortAvailable
	})
	if err != nil {
		return 0, err
	}
	return result, nil
}

// Release 归还端口；重复释放/池外端口返回错误。
func (p *FSPortPool) Release(port int) error {
	return p.withLock(func(in map[int]bool) error {
		if port < p.min || port > p.max {
			return ErrPortOutOfRange
		}
		if !in[port] {
			return ErrPortNotAllocated
		}
		delete(in, port)
		return nil
	})
}

// InUse 返回已分配端口数。
func (p *FSPortPool) InUse() int {
	n := 0
	_ = p.withLock(func(in map[int]bool) error { n = len(in); return nil })
	return n
}

// Snapshot 返回当前已分配端口的有序快照（用于以权威状态重建内存视图）。
func (p *FSPortPool) Snapshot() []int {
	out := []int{}
	_ = p.withLock(func(in map[int]bool) error {
		for port := range in {
			out = append(out, port)
		}
		return nil
	})
	sort.Ints(out)
	return out
}

// Reset 清空位图（以快照为权威重建前使用）。
func (p *FSPortPool) Reset() error {
	return p.withLock(func(in map[int]bool) error {
		for k := range in {
			delete(in, k)
		}
		return nil
	})
}
