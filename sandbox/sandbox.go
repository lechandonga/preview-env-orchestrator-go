// Package sandbox 用本地进程与文件系统模拟隔离沙箱。
package sandbox

import "os/exec"

// Process 表示沙箱内运行的一个服务进程。
type Process struct {
	Name string
	cmd  *exec.Cmd
}

// PID 返回进程号，未启动返回 -1。
func (p *Process) PID() int { return -1 }

// Alive 报告进程是否仍在运行。
func (p *Process) Alive() bool { return false }

// Stop 终止进程并等待退出，幂等。
func (p *Process) Stop() error { return nil }

// Sandbox 是一个环境实例的本地隔离单元（独立工作目录 + 进程组）。
type Sandbox struct {
	Root string
}

// New 创建沙箱目录。
func New(root string) (*Sandbox, error) { return &Sandbox{Root: root}, nil }

// Start 在沙箱内启动一个服务进程，env 为注入的环境变量。
func (s *Sandbox) Start(name string, command []string, env []string) (*Process, error) {
	return nil, nil
}

// Cleanup 停止所有进程并删除工作目录，幂等。
func (s *Sandbox) Cleanup() error { return nil }
