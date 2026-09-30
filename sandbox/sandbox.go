// Package sandbox 用本地进程与文件系统模拟隔离沙箱。
package sandbox

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// Process 表示沙箱内运行的一个服务进程。
type Process struct {
	Name string
	cmd  *exec.Cmd
	pid  int // 接管（reattach）的进程没有 cmd，用 pid + 轮询观察

	mu      sync.Mutex
	done    chan struct{}
	stopped bool
}

// PID 返回进程号，未启动返回 -1。
func (p *Process) PID() int {
	if p.cmd == nil || p.cmd.Process == nil {
		return p.pid
	}
	return p.cmd.Process.Pid
}

func (p *Process) osProcess() *os.Process {
	if p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	return p.cmd.Process
}

// Alive 报告进程是否仍在运行。
func (p *Process) Alive() bool {
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

// Stop 终止进程组并等待退出，幂等。
func (p *Process) Stop() error {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return nil
	}
	p.stopped = true
	p.mu.Unlock()
	if proc := p.osProcess(); proc != nil {
		// 杀整个进程组，避免残留子进程。
		if err := syscall.Kill(-proc.Pid, syscall.SIGKILL); err != nil {
			// 进程组可能已不存在；尝试直接杀进程本身。
			syscall.Kill(proc.Pid, syscall.SIGKILL)
		}
	} else if p.pid > 0 {
		syscall.Kill(-p.pid, syscall.SIGKILL)
	}
	<-p.done
	return nil
}

// Attach 接管一个仍在运行的外部进程（引擎重启后用）。
// 通过定期 kill(pid, 0) 观察存活；接管不持有 wait 句柄，因此只能观察、不能收割，
// Stop 时通过进程组 SIGKILL 终止。
func Attach(name string, pid int) (*Process, error) {
	if pid <= 0 {
		return nil, fmt.Errorf("attach %s: invalid pid %d", name, pid)
	}
	if err := syscall.Kill(pid, 0); err != nil {
		return nil, fmt.Errorf("attach %s pid %d: %w", name, pid, err)
	}
	p := &Process{Name: name, pid: pid, done: make(chan struct{})}
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			if syscall.Kill(pid, 0) != nil {
				close(p.done)
				return
			}
		}
	}()
	return p, nil
}

// Sandbox 是一个环境实例的本地隔离单元（独立工作目录 + 进程组）。
type Sandbox struct {
	Root string

	mu    sync.Mutex
	procs []*Process
}

// New 创建沙箱目录。
func New(root string) (*Sandbox, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("create sandbox dir: %w", err)
	}
	return &Sandbox{Root: root}, nil
}

// Start 在沙箱内启动一个服务进程，env 为注入的环境变量。
// 进程输出写入沙箱内日志文件；env（可能含密钥）绝不写入日志。
func (s *Sandbox) Start(name string, command []string, env []string) (*Process, error) {
	logPath := filepath.Join(s.Root, name+".log")
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log for %s: %w", name, err)
	}
	cmd := exec.Command(command[0], command[1:]...)
	cmd.Dir = s.Root
	cmd.Env = env
	cmd.Stdout = f
	cmd.Stderr = f
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		f.Close()
		return nil, fmt.Errorf("start %s: %w", name, err)
	}
	p := &Process{Name: name, cmd: cmd, done: make(chan struct{})}
	go func() {
		cmd.Wait()
		f.Close()
		close(p.done)
	}()
	s.mu.Lock()
	s.procs = append(s.procs, p)
	s.mu.Unlock()
	return p, nil
}

// Processes 返回沙箱内进程快照（对账用）。
func (s *Sandbox) Processes() []*Process {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*Process(nil), s.procs...)
}

// Cleanup 停止所有进程并删除工作目录，幂等。
func (s *Sandbox) Cleanup() error {
	s.mu.Lock()
	procs := append([]*Process(nil), s.procs...)
	s.procs = nil
	s.mu.Unlock()
	for _, p := range procs {
		p.Stop()
	}
	return os.RemoveAll(s.Root)
}

// Attach 在已有沙箱目录上重建句柄（重启后接管用）。
func AttachRoot(root string) *Sandbox {
	return &Sandbox{Root: root}
}

// Exists 报告沙箱目录是否仍然存在。
func (s *Sandbox) Exists() bool {
	_, err := os.Stat(s.Root)
	return err == nil
}
