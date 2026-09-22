package resources

import (
	"fmt"
	"sync"
)

// 端口分配相关的哨兵错误。
var (
	ErrPortInUse        = fmt.Errorf("port already in use")
	ErrNoPortAvailable  = fmt.Errorf("no port available in pool")
	ErrPortOutOfRange   = fmt.Errorf("port out of pool range")
	ErrPortNotAllocated = fmt.Errorf("port is not allocated")
)

// PortPool 在 [Min,Max] 闭区间内分配独占端口。
//
// 通过 BindStateDir 绑定状态目录后，分配以目录下的位图文件为权威来源，
// 使多个引擎实例共享同一范围时不分配重复端口；未绑定时退化为进程内分配。
type PortPool struct {
	mu    sync.Mutex
	min   int
	max   int
	inUse map[int]struct{}
	fs    *FSPortPool
}

// NewPortPool 创建指定范围的端口池。
func NewPortPool(minPort, maxPort int) *PortPool {
	return &PortPool{min: minPort, max: maxPort, inUse: map[int]struct{}{}}
}

// BindStateDir 绑定跨进程状态目录。
func (p *PortPool) BindStateDir(dir string) error {
	fs, err := NewFSPortPool(dir, p.min, p.max)
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.fs = fs
	p.mu.Unlock()
	return nil
}

// Allocate 分配端口：preferred>0 时必须占用该端口，冲突返回 ErrPortInUse；
// preferred==0 时从最小值开始自动选取一个空闲端口，池满返回 ErrNoPortAvailable。
func (p *PortPool) Allocate(preferred int) (int, error) {
	p.mu.Lock()
	if p.fs != nil {
		fs := p.fs
		p.mu.Unlock()
		return fs.Allocate(preferred)
	}
	defer p.mu.Unlock()
	if preferred != 0 {
		if preferred < p.min || preferred > p.max {
			return 0, errPortOutOfRange(preferred, p.min, p.max)
		}
		if _, ok := p.inUse[preferred]; ok {
			return 0, errPortInUse(preferred)
		}
		p.inUse[preferred] = struct{}{}
		return preferred, nil
	}
	for port := p.min; port <= p.max; port++ {
		if _, ok := p.inUse[port]; !ok {
			p.inUse[port] = struct{}{}
			return port, nil
		}
	}
	return 0, errNoPortAvailable(p.min, p.max)
}

// Release 归还端口；重复释放或释放池外端口必须返回错误而非静默成功。
func (p *PortPool) Release(port int) error {
	p.mu.Lock()
	if p.fs != nil {
		fs := p.fs
		p.mu.Unlock()
		return fs.Release(port)
	}
	defer p.mu.Unlock()
	if port < p.min || port > p.max {
		return errPortOutOfRange(port, p.min, p.max)
	}
	if _, ok := p.inUse[port]; !ok {
		return errPortNotAllocated(port)
	}
	delete(p.inUse, port)
	return nil
}

// InUse 返回已分配端口数。
func (p *PortPool) InUse() int {
	p.mu.Lock()
	if p.fs != nil {
		fs := p.fs
		p.mu.Unlock()
		return fs.InUse()
	}
	defer p.mu.Unlock()
	return len(p.inUse)
}

// IsAllocatedByAny 返回端口当前是否已被登记分配（归属由调用方依据快照判定）。
func (p *PortPool) IsAllocatedByAny(port int) bool {
	p.mu.Lock()
	if p.fs != nil {
		fs := p.fs
		p.mu.Unlock()
		for _, x := range fs.Snapshot() {
			if x == port {
				return true
			}
		}
		return false
	}
	defer p.mu.Unlock()
	_, ok := p.inUse[port]
	return ok
}

// ReconcileFromSnapshot 以快照中的端口集合为权威，重建文件位图（恢复路径）。
// 仅在绑定状态目录后可用；它先清空位图再写入快照端口，并校验区间与唯一性。
func (p *PortPool) ReconcileFromSnapshot(ports []int) error {
	p.mu.Lock()
	if p.fs == nil {
		p.mu.Unlock()
		// 内存模式：直接登记。
		for _, port := range ports {
			if _, err := p.Allocate(port); err != nil {
				return err
			}
		}
		return nil
	}
	fs := p.fs
	p.mu.Unlock()
	if err := fs.Reset(); err != nil {
		return err
	}
	seen := map[int]bool{}
	for _, port := range ports {
		if seen[port] {
			return fmt.Errorf("%w: duplicate port %d in snapshots", ErrPortInUse, port)
		}
		seen[port] = true
		if _, err := fs.Allocate(port); err != nil {
			return err
		}
	}
	return nil
}

// 供内存池与文件池共用的错误构造函数。
func errPortOutOfRange(port, minP, maxP int) error {
	return fmt.Errorf("%w: %d not in [%d,%d]", ErrPortOutOfRange, port, minP, maxP)
}
func errPortInUse(port int) error {
	return fmt.Errorf("%w: %d", ErrPortInUse, port)
}
func errNoPortAvailable(minP, maxP int) error {
	return fmt.Errorf("%w: range [%d,%d] exhausted", ErrNoPortAvailable, minP, maxP)
}
func errPortNotAllocated(port int) error {
	return fmt.Errorf("%w: %d", ErrPortNotAllocated, port)
}
