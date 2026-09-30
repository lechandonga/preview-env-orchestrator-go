// Package sandbox 用本地进程与文件系统模拟隔离沙箱。
package sandbox

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Process 表示沙箱内运行的一个服务进程。
type Process struct {
	Name string
	cmd  *exec.Cmd
	pid  int32 // 重新附挂（非子进程）时使用

	mu      sync.Mutex
	done    chan struct{}
	stopped bool
}

// PID 返回进程号，未启动返回 -1。
func (p *Process) PID() int {
	if p.cmd == nil || p.cmd.Process == nil {
		return int(p.pid)
	}
	return p.cmd.Process.Pid
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
	pid := p.PID()
	if pid > 0 {
		// 杀整个进程组，避免残留子进程。
		// 对非子进程（重新附挂）同样适用：服务以 Setpgid 启动，进程组首进程
		// 即服务自身，-pid 不会误伤引擎进程组。
		if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
			syscall.Kill(pid, syscall.SIGKILL)
		}
	}
	<-p.done
	return nil
}

// attach 重新附挂到一个仍在运行的服务进程（引擎重启后接管用）。
// startTicks 为该进程在 /proc/<pid>/stat 中的启动时间（clock ticks），
// 用于识别 PID 复用：PID 不存在或启动时间不匹配时返回 nil（视为已死亡）。
func attach(name string, pid int, startTicks int64) *Process {
	if pid <= 0 || !procIdentityMatches(pid, startTicks) {
		return nil
	}
	p := &Process{Name: name, pid: int32(pid), done: make(chan struct{})}
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			if !procIdentityMatches(pid, startTicks) {
				close(p.done)
				return
			}
		}
	}()
	return p
}

// procIdentityMatches 校验 pid 仍存在且启动时间戳与记录一致（防 PID 复用）。
func procIdentityMatches(pid int, startTicks int64) bool {
	if startTicks <= 0 {
		// 旧记录没有启动时间戳：退化为存活探测。
		return syscall.Kill(pid, 0) == nil
	}
	ticks, err := procStartTicks(pid)
	if err != nil {
		return false
	}
	return ticks == startTicks
}

// ProcStartTicks 读取 /proc/<pid>/stat 第 22 列（进程启动以来的 clock ticks）。
// 该值结合 PID 可在机器重启周期内唯一识别一次进程执行，防御 PID 复用。
func ProcStartTicks(pid int) (int64, error) { return procStartTicks(pid) }

func procStartTicks(pid int) (int64, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	// comm 字段（第 2 列）可能含空格与括号，先定位最后一个 ')'。
	s := string(data)
	rp := strings.LastIndexByte(s, ')')
	if rp < 0 {
		return 0, fmt.Errorf("malformed /proc/%d/stat", pid)
	}
	fields := strings.Fields(s[rp+1:])
	// ')' 之后第 1 列是 state（总第 3 列）；starttime 是总第 22 列，
	// 即此切片的索引 19（22-3）。
	const startIdx = 22 - 3
	if len(fields) <= startIdx {
		return 0, fmt.Errorf("short /proc/%d/stat", pid)
	}
	ticks, err := strconv.ParseInt(fields[startIdx], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse starttime: %w", err)
	}
	return ticks, nil
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

// StartTicks 返回已启动进程的启动时间戳；未启动返回错误。
func (p *Process) StartTicks() (int64, error) {
	pid := p.PID()
	if pid <= 0 {
		return 0, fmt.Errorf("process not started")
	}
	return procStartTicks(pid)
}

// Attach 在当前沙箱上重新附挂一个仍在运行的服务进程。
// 进程已死亡或身份不匹配（PID 复用）时返回 nil。
func (s *Sandbox) Attach(name string, pid int, startTicks int64) *Process {
	return attach(name, pid, startTicks)
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
