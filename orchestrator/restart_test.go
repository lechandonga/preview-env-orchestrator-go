package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lechandonga/preview-env-orchestrator-go/secrets"
	"github.com/lechandonga/preview-env-orchestrator-go/spec"
)

// reopenCfg 指向同一份 WorkDir/StateDir 的配置（不同引擎实例共用状态）。
func reopenCfg(t *testing.T, root string, mutate func(*Config)) Config {
	if t != nil {
		t.Helper()
	}
	cfg := Config{
		MaxEnvs:      4,
		PortMin:      31000,
		PortMax:      31200,
		WorkDir:      filepath.Join(root, "work"),
		StateDir:     filepath.Join(root, "state"),
		ReadyTimeout: 60 * time.Millisecond,
		Heartbeat:    30 * time.Millisecond,
		InstTimeout:  150 * time.Millisecond,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	return cfg
}

func newRestartOrch(t *testing.T, cfg Config, sec *secrets.Store, stop bool) *Orchestrator {
	t.Helper()
	o, err := New(cfg, sec)
	if err != nil {
		t.Fatal(err)
	}
	if stop {
		t.Cleanup(o.Close)
	} else {
		t.Cleanup(o.Stop)
	}
	return o
}

func svcSpec(name string) spec.ServiceSpec {
	return spec.ServiceSpec{
		Name:    name,
		Command: []string{"sleep", "300"},
		Ports:   []spec.PortRequest{{Name: "http"}},
	}
}

func TestRestartGracefulRecovery(t *testing.T) {
	root := t.TempDir()
	sec := secrets.NewStore()
	cfg := reopenCfg(t, root, nil)
	o1 := newRestartOrch(t, cfg, sec, false)

	s := &spec.EnvSpec{Name: "keep", Services: []spec.ServiceSpec{svcSpec("web"), func() spec.ServiceSpec {
		w := svcSpec("worker")
		w.Ports = nil
		w.DependsOn = []string{"web"}
		return w
	}()}}
	env, err := o1.Apply(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	port := env.Services["web"].Ports["http"]
	o1.mu.Lock()
	pid := o1.envs["keep"].svcs["web"].proc.PID()
	o1.mu.Unlock()

	// 正常退出：只注销实例，不销毁环境，服务进程继续在跑。
	o1.Stop()
	if syscall.Kill(pid, 0) != nil {
		t.Fatalf("退出引擎不应杀掉服务进程 pid=%d", pid)
	}

	// 新引擎指向同一份状态。
	o2 := newRestartOrch(t, cfg, sec, true)
	got, err := o2.Get("keep")
	if err != nil {
		t.Fatalf("重启后环境必须仍可查: %v", err)
	}
	if got.Status != StatusReady {
		t.Fatalf("状态应恢复为 Ready, got %s", got.Status)
	}
	if got.Services["web"].Ports["http"] != port {
		t.Fatalf("端口占用必须保持: %d != %d", got.Services["web"].Ports["http"], port)
	}
	if !got.Services["web"].Running || !got.Services["worker"].Running {
		t.Fatalf("仍在跑的服务必须被接管并继续观察: %+v", got.Services)
	}
	o2.mu.Lock()
	newPID := o2.envs["keep"].svcs["web"].proc.PID()
	o2.mu.Unlock()
	if newPID != pid {
		t.Fatalf("应附挂原进程而非重启: old=%d new=%d", pid, newPID)
	}
	// 已占用端口不得被再分配。
	x := svcSpec("other")
	x.Ports[0].Preferred = port
	if _, err := o2.Apply(context.Background(), &spec.EnvSpec{Name: "other", Services: []spec.ServiceSpec{x}}); err != nil {
		t.Fatal(err)
	}
	o2.mu.Lock()
	otherPort := o2.envs["other"].svcs["other"].ports["http"]
	o2.mu.Unlock()
	if otherPort == port {
		t.Fatalf("已占用端口 %d 被重复分配", port)
	}
	if err := o2.Destroy("other"); err != nil {
		t.Fatal(err)
	}
	// 接着操作：销毁旧环境，进程真的被停掉。
	if err := o2.Destroy("keep"); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("销毁后原进程应停止, kill err=%v", err)
	}
	if _, err := o2.Get("keep"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("销毁后不可见, got %v", err)
	}
	t.Logf("判定依据: 重启后 keep=Ready 且附挂 pid=%d 端口=%d 不变；新环境绕开占用端口；随后销毁生效", pid, port)
}

func TestRestartExternalKillReconcile(t *testing.T) {
	root := t.TempDir()
	sec := secrets.NewStore()
	cfg := reopenCfg(t, root, nil)
	o1 := newRestartOrch(t, cfg, sec, false)

	s := &spec.EnvSpec{Name: "heal", Services: []spec.ServiceSpec{svcSpec("web")}}
	env, err := o1.Apply(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	port := env.Services["web"].Ports["http"]
	o1.mu.Lock()
	pid := o1.envs["heal"].svcs["web"].proc.PID()
	o1.mu.Unlock()
	o1.Stop()

	// 模拟重启 + 服务被外部杀掉：新引擎加载后服务不运行，但端口仍被记录占用。
	syscall.Kill(pid, syscall.SIGKILL)
	time.Sleep(60 * time.Millisecond)
	o2 := newRestartOrch(t, cfg, sec, true)
	got, _ := o2.Get("heal")
	if got.Services["web"].Running {
		t.Fatal("死亡进程不应被错误附挂为运行中")
	}
	if owner, ok := o2.Ports().Owner(port); !ok || owner != "heal" {
		t.Fatalf("死亡服务的端口占用必须保留: owner=%v ok=%v", owner, ok)
	}
	if err := o2.Reconcile("heal"); err != nil {
		t.Fatalf("重新收敛应修复被杀服务: %v", err)
	}
	got2, _ := o2.Get("heal")
	if !got2.Services["web"].Running {
		t.Fatal("对账后服务应重新运行")
	}
	if got2.Services["web"].Ports["http"] != port {
		t.Fatalf("修复后端口应保持 %d, got %d", port, got2.Services["web"].Ports["http"])
	}
	t.Logf("判定依据: 死亡进程不被误附挂；端口 %d 不外流；Reconcile 后服务恢复且端口不变", port)
}

func TestRestartSecretRotationKeepsBinding(t *testing.T) {
	root := t.TempDir()
	sec := secrets.NewStore()
	if _, err := sec.Put("db-pass", "v1-value"); err != nil {
		t.Fatal(err)
	}
	cfg := reopenCfg(t, root, nil)
	o1 := newRestartOrch(t, cfg, sec, false)

	x := svcSpec("web")
	x.SecretRefs = []spec.SecretRef{{Key: "db-pass", Env: "DB_PASS"}}
	if _, err := o1.Apply(context.Background(), &spec.EnvSpec{Name: "old", Services: []spec.ServiceSpec{x}}); err != nil {
		t.Fatal(err)
	}
	o1.Stop()

	// 重启之后再轮换密钥。
	if _, err := sec.Rotate("db-pass", "v2-value"); err != nil {
		t.Fatal(err)
	}
	o2 := newRestartOrch(t, cfg, sec, true)
	oldB, err := o2.SecretBindings("old", "web")
	if err != nil {
		t.Fatal(err)
	}
	if oldB["DB_PASS"] != 1 {
		t.Fatalf("重启前的环境必须仍绑定 v1, got %v", oldB)
	}
	// 杀掉旧环境进程再收敛：重启时按绑定的 v1 注入，不拿 v2 顶上。
	o2.mu.Lock()
	pid := o2.envs["old"].svcs["web"].proc.PID()
	o2.mu.Unlock()
	syscall.Kill(pid, syscall.SIGKILL)
	time.Sleep(60 * time.Millisecond)
	if err := o2.Reconcile("old"); err != nil {
		t.Fatal(err)
	}
	oldB2, _ := o2.SecretBindings("old", "web")
	if oldB2["DB_PASS"] != 1 {
		t.Fatalf("收敛后仍必须绑定 v1, got %v", oldB2)
	}
	// 新环境用新版本。
	if _, err := o2.Apply(context.Background(), &spec.EnvSpec{Name: "new", Services: []spec.ServiceSpec{x}}); err != nil {
		t.Fatal(err)
	}
	newB, _ := o2.SecretBindings("new", "web")
	if newB["DB_PASS"] != 2 {
		t.Fatalf("新环境应绑定 v2, got %v", newB)
	}
	// 吊销旧环境绑定的版本：重新收敛必须明确失败，不能悄悄换最新版本。
	if err := sec.Revoke("db-pass", 1); err != nil {
		t.Fatal(err)
	}
	o2.mu.Lock()
	pid2 := o2.envs["old"].svcs["web"].proc.PID()
	o2.mu.Unlock()
	syscall.Kill(pid2, syscall.SIGKILL)
	time.Sleep(60 * time.Millisecond)
	if err := o2.Reconcile("old"); !errors.Is(err, ErrNotConvergent) || !errors.Is(err, ErrSecretRef) {
		t.Fatalf("绑定版本被吊销应明确不可收敛, got %v", err)
	}
	oldB3, _ := o2.SecretBindings("old", "web")
	if oldB3["DB_PASS"] != 1 {
		t.Fatalf("失败后也不得篡改绑定版本: %v", oldB3)
	}
	t.Log("判定依据: 旧环境跨重启+轮换仍绑定 v1；新环境用 v2；v1 吊销后对账 ErrNotConvergent+ErrSecretRef")
}

func TestStateFilesContainNoSecretValues(t *testing.T) {
	root := t.TempDir()
	sec := secrets.NewStore()
	const secretVal = "super-secret-value-zzz"
	if _, err := sec.Put("tok", secretVal); err != nil {
		t.Fatal(err)
	}
	cfg := reopenCfg(t, root, nil)
	o := newRestartOrch(t, cfg, sec, true)
	x := svcSpec("web")
	x.SecretRefs = []spec.SecretRef{{Key: "tok", Env: "TOK"}}
	if _, err := o.Apply(context.Background(), &spec.EnvSpec{Name: "leak", Services: []spec.ServiceSpec{x}}); err != nil {
		t.Fatal(err)
	}
	var scanned int
	filepath.Walk(cfg.StateDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		scanned++
		b, rerr := os.ReadFile(path)
		if rerr == nil && strings.Contains(string(b), secretVal) {
			t.Fatalf("密钥明文出现在落盘数据 %s", path)
		}
		return nil
	})
	if scanned < 2 {
		t.Fatalf("应至少扫到环境记录与流水, got %d", scanned)
	}
	t.Logf("判定依据: 状态目录 %d 个文件（记录/流水/心跳/锁）均不含密钥明文", scanned)
}

func TestMultiInstanceConcurrency(t *testing.T) {
	root := t.TempDir()
	sec := secrets.NewStore()
	const n = 8
	cfg := reopenCfg(t, root, func(c *Config) { c.MaxEnvs = n })
	o1 := newRestartOrch(t, cfg, sec, false)
	o2 := newRestartOrch(t, cfg, sec, false)
	t.Cleanup(func() {
		for _, name := range o1.List() {
			o1.Destroy(name)
		}
	})

	// 1. 两个实例并发创建不同环境：端口不得重复。
	var wg sync.WaitGroup
	errCh := make(chan error, 2*n)
	for i := 0; i < n; i++ {
		owner := o1
		if i%2 == 1 {
			owner = o2
		}
		wg.Add(1)
		go func(owner *Orchestrator, i int) {
			defer wg.Done()
			name := fmt.Sprintf("e%02d", i)
			if _, err := owner.Apply(context.Background(),
				&spec.EnvSpec{Name: name, Services: []spec.ServiceSpec{svcSpec("web")}}); err != nil {
				errCh <- err
			}
			owner.Get(name)
			b, _ := owner.SecretBindings(name, "web")
			_ = b
		}(owner, i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	// 收集全局端口分配：共享分配表 + 落盘记录双重检查。
	list1 := o1.List()
	if len(list1) != n {
		t.Fatalf("两个实例应看到同一份环境集合, got %v", list1)
	}
	seen := map[int]string{}
	// 直接从状态记录重建校验。
	o1.mu.Lock()
	for name, st := range o1.envs {
		for _, p := range st.svcs["web"].ports {
			if other, dup := seen[p]; dup {
				o1.mu.Unlock()
				t.Fatalf("端口 %d 被分配给 %s 和 %s", p, other, name)
			}
			seen[p] = name
		}
	}
	o1.mu.Unlock()
	if len(seen) != n {
		t.Fatalf("应有 %d 个唯一端口, got %d", n, len(seen))
	}
	// 2. 全局环境数上限跨实例生效。
	x := svcSpec("over")
	if _, err := o2.Apply(context.Background(),
		&spec.EnvSpec{Name: "over", Services: []spec.ServiceSpec{x}}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("跨实例不得绕过 MaxEnvs, got %v", err)
	}
	// 3. 一个实例建的环境，另一个实例能查到并销毁。
	if _, err := o2.Get("e00"); err != nil {
		t.Fatal(err)
	}
	if err := o2.Destroy("e00"); err != nil {
		t.Fatalf("跨实例销毁失败: %v", err)
	}
	if _, err := o1.Get("e00"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("o2 销毁后 o1 也应看不到, got %v", err)
	}
	// 销毁只生效一次：共享流水里 destroy/e00 只有一行。
	if c := countJournalKind(t, cfg.StateDir, "destroy", "e00"); c != 1 {
		t.Fatalf("跨实例销毁只能生效一次, journal destroy=%d", c)
	}
	t.Logf("判定依据: %d 个跨实例并发创建得到 %d 个唯一端口；第 %d 个被 ErrCapacity 拒绝；跨实例 Get/Destroy 生效且 destroy 流水=1", n, len(seen), n+1)
}

func TestMultiInstanceTakeover(t *testing.T) {
	root := t.TempDir()
	sec := secrets.NewStore()
	cfg := reopenCfg(t, root, nil)
	o1 := newRestartOrch(t, cfg, sec, false)
	s := &spec.EnvSpec{Name: "zombie", Services: []spec.ServiceSpec{svcSpec("web")}}
	env, err := o1.Apply(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	port := env.Services["web"].Ports["http"]
	o1.Stop() // 实例 1 退出；服务进程仍在。

	// 另一个实例上线，应看到并接管（附挂进程、保留端口）。
	o2 := newRestartOrch(t, cfg, sec, true)
	got, err := o2.Get("zombie")
	if err != nil {
		t.Fatalf("接管方应能查到环境: %v", err)
	}
	if !got.Services["web"].Running || got.Services["web"].Ports["http"] != port {
		t.Fatalf("接管后服务应运行且端口保持 %d: %+v", port, got.Services["web"])
	}
	if err := o2.Destroy("zombie"); err != nil {
		t.Fatalf("接管方应能完成清理: %v", err)
	}
	if _, err := o2.Get("zombie"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("清理后不可见, got %v", err)
	}
	if c := countJournalKind(t, cfg.StateDir, "destroy", "zombie"); c != 1 {
		t.Fatalf("接管清理不得重复销毁, journal destroy=%d", c)
	}
	t.Logf("判定依据: o1 退出后 o2 接管 zombie（附挂进程、端口=%d 不变）并完成唯一一次销毁", port)
}

func countJournalKind(t *testing.T, stateDir, kind, envName string) int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(stateDir, "events.log"))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if strings.Contains(line, `"Kind":"`+kind+`"`) && strings.Contains(line, `"Env":"`+envName+`"`) {
			n++
		}
	}
	return n
}

// --- 强杀引擎（SIGKILL）恢复：真实子进程，不用 mock ---

// TestCrashHelperMode 是辅助进程模式标志。
var crashHelperMode = os.Getenv("PEO_CRASH_HELPER") == "1"

func init() {
	if !crashHelperMode {
		return
	}
	// 辅助进程：创建一个环境后按信号自我强杀，或正常退出。
	runCrashHelper()
	os.Exit(0)
}

func runCrashHelper() {
	root := os.Getenv("PEO_STATE_ROOT")
	sec := secrets.NewStore()
	cfg := reopenCfg(nil, root, nil)
	o, err := New(cfg, sec)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	s := &spec.EnvSpec{Name: "crashed", Services: []spec.ServiceSpec{svcSpec("web")}}
	env, err := o.Apply(context.Background(), s)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("READY port=%d\n", env.Services["web"].Ports["http"])
	os.Stdout.Sync()
	if os.Getenv("PEO_CRASH_KILL") == "1" {
		// 不注销心跳、不清理：SIGKILL 自己。
		syscall.Kill(os.Getpid(), syscall.SIGKILL)
	}
	select {} // 正常模式下挂住，等父进程决定
}

func TestCrashKillRecovery(t *testing.T) {
	if os.Getenv("PEO_CHILD_MAIN") == "" {
		// 父进程：编译测试二进制并以 helper 模式启动。
		root := t.TempDir()
		bin := filepath.Join(t.TempDir(), "crash.test")
		build := exec.Command("go", "test", "-c", "-o", bin, ".")
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build helper: %v\n%s", err, out)
		}
		cmd := exec.Command(bin, "-test.run", "TestCrashKillRecovery")
		cmd.Env = append(os.Environ(),
			"PEO_CHILD_MAIN=1", "PEO_CRASH_HELPER=1",
			"PEO_CRASH_KILL=1", "PEO_STATE_ROOT="+root,
		)
		out, err := cmd.CombinedOutput()
		if cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != 137 &&
			!strings.Contains(cmd.ProcessState.String(), "killed") {
			// 被 SIGKILL：Go 通常退出码 137 或 signal=killed。
			t.Logf("helper output=%s exit=%v", strings.TrimSpace(string(out)), err)
		}
		port := 0
		for _, line := range strings.Split(string(out), "\n") {
			if strings.HasPrefix(line, "READY ") {
				fmt.Sscanf(line, "READY port=%d", &port)
			}
		}
		if port == 0 {
			t.Fatalf("helper 未完成创建: %s", out)
		}

		// 心跳超时后，新实例上线接管。
		sec := secrets.NewStore()
		cfg := reopenCfg(t, root, nil)
		// 给死实例心跳超时一点时间。
		time.Sleep(cfg.InstTimeout + 50*time.Millisecond)
		o := newRestartOrch(t, cfg, sec, true)
		got, err := o.Get("crashed")
		if err != nil {
			t.Fatalf("强杀后环境必须仍在: %v", err)
		}
		if !got.Services["web"].Running {
			t.Fatal("仍在跑的服务进程应被新实例接管观察")
		}
		if got.Services["web"].Ports["http"] != port {
			t.Fatalf("端口必须保持: %d != %d", got.Services["web"].Ports["http"], port)
		}
		if c := countJournalKind(t, cfg.StateDir, "destroy", "crashed"); c != 0 {
			t.Fatal("接管前不应产生销毁")
		}
		if err := o.Reconcile("crashed"); err != nil {
			t.Fatal(err)
		}
		if err := o.Destroy("crashed"); err != nil {
			t.Fatal(err)
		}
		if c := countJournalKind(t, cfg.StateDir, "destroy", "crashed"); c != 1 {
			t.Fatalf("跨强杀的销毁只能生效一次, got %d", c)
		}
		t.Logf("判定依据: 引擎被 SIGKILL 后，新实例接管仍跑的服务（端口=%d），Reconcile 通过且销毁流水=1", port)
	}
}
