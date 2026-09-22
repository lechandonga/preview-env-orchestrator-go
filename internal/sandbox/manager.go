// Package sandbox 用本地进程与文件系统模拟隔离沙箱：
// 每个被启动的服务对应一个真实但无副作用的本地子进程（sleep）与 PID 文件。
//
// 设计取舍：
//   - 使用真实进程（os/exec 启动 sleep），因此进程被杀、被外部改动后可以被
//     真实地观察到，对账逻辑不是自说自话；
//   - 密钥值仅通过进程环境变量注入，不写入任何文件；
//   - 启动失败、就绪超时、外部 kill 均可稳定模拟，供测试在竞态检测下复现。
package sandbox

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

// 哨兵错误。
var (
	ErrProcessDead    = errors.New("sandbox process exited before becoming ready")
	ErrNoPIDFile      = errors.New("no pid file for process")
	ErrStalePIDFile   = errors.New("pid file references a non-existent/foreign process")
	ErrAlreadyStopped = errors.New("sandbox process already stopped")
)

// Manager 管理模拟沙箱进程的生命周期。
type Manager struct {
	mu       sync.Mutex
	stateDir string

	// failRemoveEnvDir 非空时，RemoveEnvDir 返回该错误（仅用于测试回滚失败路径）。
	failRemoveEnvDir error
}

// SetFailRemoveEnvDir 注入 RemoveEnvDir 故障（测试专用）；传 nil 解除。
func (m *Manager) SetFailRemoveEnvDir(err error) {
	m.mu.Lock()
	m.failRemoveEnvDir = err
	m.mu.Unlock()
}

// NewManager 创建沙箱管理器，PID 文件写入 stateDir。
func NewManager(stateDir string) *Manager {
	return &Manager{stateDir: stateDir}
}

// StartRequest 描述一次服务启动请求（含解析后的密钥值，仅注入进程环境，不落盘）。
type StartRequest struct {
	EnvName   string
	Service   string
	Image     string
	Env       map[string]string
	SecretEnv map[string]string
	Fail      bool // 模拟启动失败：进程立即以非零码退出
}

// Start 启动服务进程，返回 PID 并写入 PID 文件。
func (m *Manager) Start(req StartRequest) (int, error) {
	if req.Fail {
		// 以退出码 7 立即失败，模拟容器启动失败。
		cmd := exec.Command("sh", "-c", "exit 7")
		cmd.Env = os.Environ()
		if err := cmd.Run(); err != nil {
			return 0, fmt.Errorf("%w: service %s start script failed: %v", ErrProcessDead, req.Service, err)
		}
		return 0, fmt.Errorf("%w: service %s start script failed", ErrProcessDead, req.Service)
	}

	// 以 sleep 承载“常驻进程”，无任何外部副作用。
	cmd := exec.Command("sleep", "3600")
	cmd.Env = os.Environ()
	for k, v := range req.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	// 密钥值只存在于进程环境中。
	for k, v := range req.SecretEnv {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("start sandbox process for %s/%s: %w", req.EnvName, req.Service, err)
	}
	pid := cmd.Process.Pid

	// 后台回收僵尸进程，避免泄漏。
	go func() { _ = cmd.Wait() }()

	dir := m.envDir(req.EnvName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		_ = killPID(pid)
		return 0, fmt.Errorf("create sandbox dir: %w", err)
	}
	pidFile := m.pidFile(req.EnvName, req.Service)
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(pid)), 0o600); err != nil {
		_ = killPID(pid)
		return 0, fmt.Errorf("write pid file: %w", err)
	}
	return pid, nil
}

// IsReady 查询进程是否仍存活（模拟就绪探针：进程在即为就绪）。
func (m *Manager) IsReady(pid int) bool { return pidAlive(pid) }

// Stop 终止进程并删除其 PID 文件；重复停止必须返回错误而不是静默成功，
// 使“资源已释放”成为可区分事件，支撑回滚不重复释放的要求。
func (m *Manager) Stop(envName, service string, pid int) error {
	pidFile := m.pidFile(envName, service)
	data, err := os.ReadFile(pidFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// PID 文件已不在：可能已被回滚，确认进程也不在后给出可区分错误。
			if !pidAlive(pid) {
				return fmt.Errorf("%w: %s/%s pid=%d", ErrAlreadyStopped, envName, service, pid)
			}
			// 文件丢失但进程还在：清理进程并返回陈旧文件错误，供对账使用。
			_ = killPID(pid)
			return fmt.Errorf("%w: %s/%s (process killed)", ErrStalePIDFile, envName, service)
		}
		return err
	}
	filePID, perr := strconv.Atoi(strings.TrimSpace(string(data)))
	if perr != nil || filePID != pid {
		return fmt.Errorf("%w: %s/%s wants pid=%d but file says %q", ErrStalePIDFile, envName, service, pid, string(data))
	}
	if err := killPID(pid); err != nil {
		return err
	}
	if err := os.Remove(pidFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove pid file: %w", err)
	}
	return nil
}

// StopByPIDFile 依据 PID 文件停止进程（对账/恢复路径使用）。
func (m *Manager) StopByPIDFile(envName, service string) error {
	pidFile := m.pidFile(envName, service)
	data, err := os.ReadFile(pidFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: %s/%s", ErrNoPIDFile, envName, service)
		}
		return err
	}
	pid, perr := strconv.Atoi(strings.TrimSpace(string(data)))
	if perr != nil {
		return fmt.Errorf("%w: %s/%s bad pid %q", ErrStalePIDFile, envName, service, string(data))
	}
	if !pidAlive(pid) {
		// 进程已被外部杀死：清理陈旧 PID 文件并报告。
		_ = os.Remove(pidFile)
		return fmt.Errorf("%w: %s/%s pid=%d (stale file removed)", ErrStalePIDFile, envName, service, pid)
	}
	return m.Stop(envName, service, pid)
}

// ListPIDs 列出某环境目录下 PID 文件记录的全部 (服务名, PID)，含已失效的条目。
func (m *Manager) ListPIDs(envName string) (map[string]int, error) {
	dir := m.envDir(envName)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]int{}, nil
		}
		return nil, err
	}
	out := map[string]int{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".pid") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil {
			continue
		}
		out[strings.TrimSuffix(e.Name(), ".pid")] = pid
	}
	return out, nil
}

// RemoveEnvDir 清理某环境的沙箱目录（仅应在所有进程停止后调用）。
func (m *Manager) RemoveEnvDir(envName string) error {
	m.mu.Lock()
	injected := m.failRemoveEnvDir
	m.mu.Unlock()
	if injected != nil {
		return injected
	}
	return os.RemoveAll(m.envDir(envName))
}

func (m *Manager) envDir(envName string) string {
	return filepath.Join(m.stateDir, "sandbox", safeName(envName))
}

func (m *Manager) pidFile(envName, service string) string {
	return filepath.Join(m.envDir(envName), safeName(service)+".pid")
}

func safeName(s string) string {
	r := strings.NewReplacer("/", "_", "..", "_", " ", "_")
	return r.Replace(s)
}

// pidAlive 通过 signal 0 探测进程是否存活。
func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	if err := syscall.Kill(pid, 0); err != nil {
		// ESRCH=不存在；EPERM=存在但无权限（仍视为存在）。
		return errors.Is(err, syscall.EPERM)
	}
	return true
}

func killPID(pid int) error {
	if !pidAlive(pid) {
		return nil
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("kill pid %d: %w", pid, err)
	}
	return nil
}
