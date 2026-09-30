package orchestrator

import (
	"bufio"
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

	"github.com/lechandonga/preview-env-orchestrator-go/ports"
	"github.com/lechandonga/preview-env-orchestrator-go/secrets"
	"github.com/lechandonga/preview-env-orchestrator-go/spec"
)

// restartCfg 构造一份指向独立目录的配置（重启/多实例测试用）。
func restartCfg(t *testing.T, mutate func(*Config)) Config {
	t.Helper()
	cfg := Config{
		MaxEnvs:      4,
		PortMin:      33000,
		PortMax:      33200,
		WorkDir:      filepath.Join(t.TempDir(), "work"),
		ReadyTimeout: 60 * time.Millisecond,
	}
	cfg.StateDir = filepath.Join(cfg.WorkDir, "state")
	if mutate != nil {
		mutate(&cfg)
	}
	return cfg
}

func secretStoreWithToken() *secrets.Store {
	sec := secrets.NewStore()
	sec.Put("tok", "v1-secret")
	return sec
}

func envWithPortSecret(name string) *spec.EnvSpec {
	x := svc("web")
	x.Ports = []spec.PortRequest{{Name: "http", Preferred: 33100}}
	x.SecretRefs = []spec.SecretRef{{Key: "tok", Env: "TOK"}}
	return &spec.EnvSpec{Name: name, Services: []spec.ServiceSpec{x}}
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timeout: " + msg)
}

// TestRestartRecoveryGraceful 正常退出（Shutdown 不销毁）后新实例接管：
// 环境、状态、端口、配额、流水都还在，并能继续操作与销毁。
func TestRestartRecoveryGraceful(t *testing.T) {
	cfg := restartCfg(t, nil)
	o1, err := New(cfg, secretStoreWithToken())
	if err != nil {
		t.Fatal(err)
	}
	env, err := o1.Apply(context.Background(), envWithPortSecret("resume"))
	if err != nil {
		t.Fatal(err)
	}
	port := env.Services["web"].Ports["http"]
	allocCount := o1.Ledger().Count("alloc-port")

	o1.Shutdown() // 正常退出：状态保留，服务进程继续运行

	o2, err := New(cfg, secretStoreWithToken())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o2.Close)

	got, err := o2.Get("resume")
	if err != nil {
		t.Fatalf("重启后环境应仍可查: %v", err)
	}
	if got.Status != StatusReady || !got.Services["web"].Running {
		t.Fatalf("接管后应 Ready/Running, got %s running=%v", got.Status, got.Services["web"].Running)
	}
	if got.Services["web"].Ports["http"] != port {
		t.Fatalf("端口绑定应保持: %d vs %d", got.Services["web"].Ports["http"], port)
	}
	if owner, ok := o2.Ports().Owner(port); !ok || owner != "resume" {
		t.Fatalf("端口仍应归属 resume: %v", owner)
	}
	if got := o2.List(); len(got) != 1 || got[0] != "resume" {
		t.Fatalf("环境数（配额占用）应恢复: %v", got)
	}
	b, _ := o2.SecretBindings("resume", "web")
	if b["TOK"] != 1 {
		t.Fatalf("密钥绑定应恢复为引用 v1: %v", b)
	}
	// 历史流水必须可见，且重启没有重复分配。
	if o2.Ledger().Count("alloc-port") != allocCount {
		t.Fatalf("重启不得重复分配端口: before=%d after=%d", allocCount, o2.Ledger().Count("alloc-port"))
	}

	if err := o2.Destroy("resume"); err != nil {
		t.Fatalf("新实例应能继续销毁: %v", err)
	}
	if _, err := o2.Get("resume"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("销毁后应不可见: %v", err)
	}
	if n := o2.Ports().InUse(); n != 0 {
		t.Fatalf("端口零残留: %d", n)
	}
	if got := o2.Ledger().Count("destroy"); got != 1 {
		t.Fatalf("销毁只能生效一次: %d", got)
	}
	t.Logf("判定依据: Shutdown 后新实例接管 env=resume 端口=%d 状态=Ready; 跨实例销毁 destroy=1, 端口残留=0", port)
}

// TestRestartHardKillTakeover 子进程（另一个引擎实例）被 SIGKILL 后：
// 仍在跑的服务被新实例接管观察；外部杀掉服务后 Reconcile 能修复；端口不重复分配。
func TestRestartHardKillTakeover(t *testing.T) {
	cfg := restartCfg(t, nil)

	helper := exec.Command(os.Args[0], "-test.run=TestHelperProcess")
	helper.Env = append(os.Environ(),
		"HELPER=1",
		"HELPER_WORKDIR="+cfg.WorkDir,
		"HELPER_STATEDIR="+cfg.StateDir,
	)
	stdout, err := helper.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := helper.Start(); err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(stdout)
	if !sc.Scan() || strings.TrimSpace(sc.Text()) != "READY" {
		t.Fatalf("helper 未就绪: %v", sc.Err())
	}
	// 强杀引擎进程：服务进程（独立进程组）应存活。
	if err := helper.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	helper.Wait()

	o, err := New(cfg, secretStoreWithToken())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o.Close)

	env, err := o.Get("hk")
	if err != nil {
		t.Fatalf("强杀后环境必须还在: %v", err)
	}
	if !env.Services["web"].Running {
		t.Fatal("仍在运行的服务进程应被接管并继续观察")
	}
	port := env.Services["web"].Ports["http"]

	// 外部杀掉服务进程，重新收敛必须修复。
	o.mu.Lock()
	pid := o.envs["hk"].svcs["web"].proc.PID()
	o.mu.Unlock()
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		cur, _ := o.Get("hk")
		return cur != nil && !cur.Services["web"].Running
	}, "等待外部杀进程被观察到")
	if err := o.Reconcile("hk"); err != nil {
		t.Fatalf("对账应修复被杀服务: %v", err)
	}
	env2, err := o.Get("hk")
	if err != nil {
		t.Fatal(err)
	}
	if !env2.Services["web"].Running {
		t.Fatal("服务应被对账重启")
	}
	o.mu.Lock()
	newPID := o.envs["hk"].svcs["web"].proc.PID()
	o.mu.Unlock()
	if newPID == pid {
		t.Fatal("应是新进程")
	}
	if env2.Services["web"].Ports["http"] != port {
		t.Fatalf("修复后端口绑定应保持: %d vs %d", env2.Services["web"].Ports["http"], port)
	}
	if owner, _ := o.Ports().Owner(port); owner != "hk" {
		t.Fatal("端口不得被重复分配给别人")
	}
	t.Logf("判定依据: 引擎被 SIGKILL 后服务 pid=%d 被接管; 外部杀服务后对账重启为 pid=%d, 端口=%d 不变", pid, newPID, port)
}

// TestOrphanCreateCleanupOnRestart 崩溃在 creating 阶段的记录：
// 重启时释放其端口、删除沙箱与记录，不留孤儿资源、不占配额。
func TestOrphanCreateCleanupOnRestart(t *testing.T) {
	cfg := restartCfg(t, nil)
	if err := os.MkdirAll(cfg.StateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	sandboxDir := filepath.Join(cfg.WorkDir, "half")
	if err := os.MkdirAll(sandboxDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sandboxDir, "junk"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := &envRecord{
		Version: stateVersion,
		Name:    "half",
		Spec: spec.EnvSpec{Name: "half", Services: []spec.ServiceSpec{{
			Name:    "web",
			Command: []string{"sleep", "30"},
		}}},
		SpecKey:   "deadbeef",
		Phase:     phaseCreating,
		Status:    StatusStarting,
		CreatedAt: time.Now(),
		Services: []svcRecord{{
			Name:  "web",
			Ports: map[string]int{"http": 33150},
		}},
	}
	pa := ports.New(cfg.PortMin, cfg.PortMax)
	pa.Allocate("half", 33150)
	if err := pa.Save(filepath.Join(cfg.StateDir, portsFileName)); err != nil {
		t.Fatal(err)
	}
	if err := saveRecord(cfg.StateDir, r); err != nil {
		t.Fatal(err)
	}

	o, err := New(cfg, secretStoreWithToken())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o.Close)

	if _, err := o.Get("half"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("半截创建应被清理, got %v", err)
	}
	if _, ok := o.Ports().Owner(33150); ok {
		t.Fatal("孤儿环境的端口必须被释放，不能再次分配给它人之前先占用")
	}
	if _, err := os.Stat(sandboxDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("孤儿沙箱应被删除: %v", err)
	}
	if n := o.Ledger().Count("orphan-abandoned"); n != 1 {
		t.Fatalf("应记录一次孤儿清理, got %d", n)
	}
	// 被释放的端口可以重新分配给新环境。
	e, err := o.Apply(context.Background(), envWithPortSecret("fresh"))
	if err != nil {
		t.Fatal(err)
	}
	if e.Services["web"].Ports["http"] != 33100 {
		t.Fatalf("首选端口 33100 应可用, got %d", e.Services["web"].Ports["http"])
	}
	t.Log("判定依据: creating 孤儿记录在 New 时被清理（端口释放、沙箱删除、配额不占），新环境正常创建")
}

// TestRestartSecretRotationBinding 重启后轮换密钥：旧环境坚持原绑定版本，
// 新环境用新版本；旧绑定版本被吊销时对账明确失败，不静默拿最新。
func TestRestartSecretRotationBinding(t *testing.T) {
	cfg := restartCfg(t, nil)
	sec := secretStoreWithToken()
	o1, err := New(cfg, sec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o1.Apply(context.Background(), envWithPortSecret("old")); err != nil {
		t.Fatal(err)
	}
	// 取到服务 PID 并记录端口后正常退出。
	env1, _ := o1.Get("old")
	oldPort := env1.Services["web"].Ports["http"]
	o1.Shutdown()

	if _, err := sec.Rotate("tok", "v2-secret"); err != nil {
		t.Fatal(err)
	}
	o2, err := New(cfg, sec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o2.Close)

	// 旧环境绑定仍为 v1（不因轮换而改变）。
	oldB, err := o2.SecretBindings("old", "web")
	if err != nil || oldB["TOK"] != 1 {
		t.Fatalf("重启后旧环境绑定必须保持 v1: %v %v", oldB, err)
	}
	// 新建环境绑定新版本 v2。
	e2, err := o2.Apply(context.Background(), func() *spec.EnvSpec {
		s := envWithPortSecret("newer")
		s.Services[0].Ports = []spec.PortRequest{{Name: "http", Preferred: 33101}}
		return s
	}())
	if err != nil {
		t.Fatal(err)
	}
	_ = e2
	newB, _ := o2.SecretBindings("newer", "web")
	if newB["TOK"] != 2 {
		t.Fatalf("新建环境应绑定 v2: %v", newB)
	}
	// 落盘内容不得包含任何密钥值。
	if data, _ := os.ReadFile(filepath.Join(cfg.StateDir, "env-old.json")); strings.Contains(string(data), "v1-secret") || strings.Contains(string(data), "v2-secret") {
		t.Fatalf("状态文件出现密钥明文: %s", data)
	}

	// 外部杀掉旧服务后对账：原绑定 v1 仍有效，应修复成功。
	o2.mu.Lock()
	pid := o2.envs["old"].svcs["web"].proc.PID()
	o2.mu.Unlock()
	syscall.Kill(pid, syscall.SIGKILL)
	waitFor(t, func() bool {
		cur, _ := o2.Get("old")
		return cur != nil && !cur.Services["web"].Running
	}, "等待旧服务退出")
	if err := o2.Reconcile("old"); err != nil {
		t.Fatalf("原绑定有效时应可修复: %v", err)
	}
	oldB2, _ := o2.SecretBindings("old", "web")
	if oldB2["TOK"] != 1 {
		t.Fatalf("修复后仍必须坚持 v1 绑定: %v", oldB2)
	}
	if cur, _ := o2.Get("old"); cur.Services["web"].Ports["http"] != oldPort {
		t.Fatalf("端口应保持 %d: %d", oldPort, cur.Services["web"].Ports["http"])
	}

	// 模拟绑定版本被吊销/引用失效：存储中该密钥引用完全不存在。
	secRevoked := secrets.NewStore()
	o3, err := New(cfg, secRevoked)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o3.Close)
	o3.mu.Lock()
	pid2 := o3.envs["old"].svcs["web"].proc.PID()
	o3.mu.Unlock()
	syscall.Kill(pid2, syscall.SIGKILL)
	waitFor(t, func() bool {
		cur, _ := o3.Get("old")
		return cur != nil && !cur.Services["web"].Running
	}, "等待旧服务退出（吊销场景）")
	err = o3.Reconcile("old")
	if !errors.Is(err, ErrNotConvergent) {
		t.Fatalf("绑定失效必须明确不可收敛, got %v", err)
	}
	if !errors.Is(err, ErrSecretRef) {
		t.Fatalf("绑定失效必须明确不可收敛且为 ErrSecretRef, got %v", err)
	}
	t.Logf("判定依据: 重启后旧环境坚持 v1，新建环境绑定 v2；v1 吊销后对账返回 %v，绝不静默用最新", err)
}

// TestMultiInstanceSharedState 两个引擎实例共用一份状态：
// 并发创建/销毁/查询时上限、端口全局一致；一个实例建的另一个可见可销毁。
func TestMultiInstanceSharedState(t *testing.T) {
	cfg := restartCfg(t, func(c *Config) { c.MaxEnvs = 4 })
	sec := secretStoreWithToken()
	a, err := New(cfg, sec)
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(cfg, sec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	t.Cleanup(b.Close)

	const n = 8
	var wg sync.WaitGroup
	portsSeen := sync.Mutex{}
	seen := map[int]string{}
	errs := make(chan error, n*2)
	for i := 0; i < n; i++ {
		owner := a
		if i%2 == 1 {
			owner = b
		}
		wg.Add(1)
		go func(i int, o *Orchestrator) {
			defer wg.Done()
			s := &spec.EnvSpec{Name: fmt.Sprintf("mi-%d", i), Services: []spec.ServiceSpec{
				func() spec.ServiceSpec {
					x := svc("web")
					x.Ports = []spec.PortRequest{{Name: "http"}}
					return x
				}(),
			}}
			env, err := o.Apply(context.Background(), s)
			if err != nil {
				errs <- err
				return
			}
			p := env.Services["web"].Ports["http"]
			portsSeen.Lock()
			if prev, dup := seen[p]; dup {
				errs <- fmt.Errorf("端口 %d 同时分给 %s 和 mi-%d", p, prev, i)
			}
			seen[p] = fmt.Sprintf("mi-%d", i)
			portsSeen.Unlock()
		}(i, owner)
	}
	wg.Wait()
	close(errs)
	var capacityRejects int
	for e := range errs {
		if errors.Is(e, ErrCapacity) {
			capacityRejects++
			continue
		}
		t.Fatal(e)
	}
	listA := a.List()
	listB := b.List()
	if len(listA) != cfg.MaxEnvs || len(listB) != cfg.MaxEnvs {
		t.Fatalf("两实例看到的环境数必须一致且等于上限: a=%v b=%v", listA, listB)
	}
	if a.Ports().InUse() != b.Ports().InUse() {
		t.Fatalf("两实例端口占用视图不一致: %d vs %d", a.Ports().InUse(), b.Ports().InUse())
	}
	// 超出上限必须被拒绝（8 个申请，上限 4）。
	if capacityRejects != n-cfg.MaxEnvs {
		t.Fatalf("应有 %d 个因上限被拒, got %d", n-cfg.MaxEnvs, capacityRejects)
	}

	// b 销毁 a 建的环境，a 立即可见消失；再由 a 创建补位成功。
	var crossName string
	for _, name := range listA {
		if _, err := b.Get(name); err == nil {
			crossName = name
			break
		}
	}
	if crossName == "" {
		t.Fatal("一个实例应能查到另一个实例建的环境")
	}
	if err := b.Destroy(crossName); err != nil {
		t.Fatalf("实例 b 应能销毁 a 建的 %s: %v", crossName, err)
	}
	if _, err := a.Get(crossName); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a 应立即可见 %s 消失: %v", crossName, err)
	}
	s := &spec.EnvSpec{Name: "backfill", Services: []spec.ServiceSpec{svc("web")}}
	if _, err := a.Apply(context.Background(), s); err != nil {
		t.Fatalf("槽位释放后应能补位创建: %v", err)
	}
	if _, err := b.Get("backfill"); err != nil {
		t.Fatalf("b 应能查到 a 补位创建的环境: %v", err)
	}
	// 并发销毁同一环境只生效一次。
	var dwg sync.WaitGroup
	for i := 0; i < 8; i++ {
		dwg.Add(2)
		go func() { defer dwg.Done(); _ = a.Destroy("backfill") }()
		go func() { defer dwg.Done(); _ = b.Destroy("backfill") }()
	}
	dwg.Wait()
	destroyInFile := countLedgerFile(t, filepath.Join(cfg.StateDir, ledgerFileName), "destroy")
	if destroyInFile != 2 {
		t.Fatalf("跨实例销毁必须只生效一次/环境, 文件中 destroy=%d", destroyInFile)
	}
	t.Logf("判定依据: 两实例共享上限=%d（拒绝 %d），端口无重复分配，跨实例可见/销毁/补位成功, destroy 流水=%d", cfg.MaxEnvs, capacityRejects, destroyInFile)
}

func countLedgerFile(t *testing.T, path, kind string) int {
	t.Helper()
	n := 0
	for _, e := range loadLedgerFile(path) {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

// TestHardKillMidDestroy 实例被强杀在销毁中途：另一个实例安全接管完成清理。
func TestHardKillMidDestroyAfterTakeover(t *testing.T) {
	cfg := restartCfg(t, nil)
	o1, err := New(cfg, secretStoreWithToken())
	if err != nil {
		t.Fatal(err)
	}
	env, err := o1.Apply(context.Background(), envWithPortSecret("mid"))
	if err != nil {
		t.Fatal(err)
	}
	port := env.Services["web"].Ports["http"]
	o1.mu.Lock()
	pid := o1.envs["mid"].svcs["web"].proc.PID()
	st := o1.envs["mid"]
	st.destroying = true
	st.env.Status = StatusDestroying
	// 只写阶段标记、不做完 teardown —— 模拟在销毁中途被强杀。
	if err := saveRecord(cfg.StateDir, o1.recordLocked(st)); err != nil {
		o1.mu.Unlock()
		t.Fatal(err)
	}
	o1.mu.Unlock()
	// 释放本进程的锁句柄（模拟进程消失），但不杀服务。
	o1.Shutdown()
	if syscall.Kill(pid, 0) != nil {
		t.Fatal("服务进程在模拟崩溃时应仍存活")
	}

	o2, err := New(cfg, secretStoreWithToken())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o2.Close)
	if _, err := o2.Get("mid"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("半截销毁应在启动时被完成清理: %v", err)
	}
	if _, ok := o2.Ports().Owner(port); ok {
		t.Fatal("半截销毁环境的端口必须已释放")
	}
	if syscall.Kill(pid, 0) == nil {
		t.Fatal("遗留服务进程必须被停掉")
	}
	if n := countLedgerFile(t, filepath.Join(cfg.StateDir, ledgerFileName), "destroy-resume"); n != 1 {
		t.Fatalf("应记录一次销毁续做, got %d", n)
	}
	t.Logf("判定依据: destroying 半截记录在新实例 New 时被完成（停进程 pid=%d、释放端口 %d、删记录）", pid, port)
}

// TestRestartExpiryReap 到期回收跨重启只生效一次：
// 重启前到期但未回收的环境，新实例 ReapExpired 完成且只回收一次。
func TestRestartExpiryReap(t *testing.T) {
	cfg := restartCfg(t, nil)
	o1, err := New(cfg, secretStoreWithToken())
	if err != nil {
		t.Fatal(err)
	}
	web := svc("web")
	web.Ports = []spec.PortRequest{{Name: "http", Preferred: 33130}}
	s := &spec.EnvSpec{Name: "ttl", TTL: 5 * time.Millisecond, Services: []spec.ServiceSpec{web}}
	if _, err := o1.Apply(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	o1.Shutdown()
	time.Sleep(20 * time.Millisecond)

	o2, err := New(cfg, secretStoreWithToken())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o2.Close)
	o2.ReapExpired(time.Now())
	o2.ReapExpired(time.Now()) // 幂等：第二次不得重复释放
	if _, err := o2.Get("ttl"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("到期环境应在重启后被回收: %v", err)
	}
	if n := o2.Ports().InUse(); n != 0 {
		t.Fatalf("端口零残留: %d", n)
	}
	if n := countLedgerFile(t, filepath.Join(cfg.StateDir, ledgerFileName), "reap"); n != 1 {
		t.Fatalf("到期回收只能生效一次: %d", n)
	}
	t.Log("判定依据: 重启后到期环境被回收且 reap=1, 二次回收无重复释放")
}

// TestPersistedStateHasNoSecretValues 落盘数据、日志、沙箱产物中不得出现密钥值。
func TestPersistedStateHasNoSecretValues(t *testing.T) {
	cfg := restartCfg(t, nil)
	const secretVal = "super-secret-value-zzz"
	sec := secrets.NewStore()
	sec.Put("tok", secretVal)
	o, err := New(cfg, sec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o.Close)
	web := svc("web")
	web.SecretRefs = []spec.SecretRef{{Key: "tok", Env: "TOK"}}
	if _, err := o.Apply(context.Background(), &spec.EnvSpec{Name: "leak2", Services: []spec.ServiceSpec{web}}); err != nil {
		t.Fatal(err)
	}
	// 1. 状态目录（环境记录、端口表、流水、锁）。
	var scanned int
	filepath.Walk(cfg.StateDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		scanned++
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil
		}
		if strings.Contains(string(b), secretVal) {
			t.Fatalf("密钥明文出现在落盘文件 %s", path)
		}
		return nil
	})
	if scanned < 3 {
		t.Fatalf("应至少扫描到记录/端口/流水文件, got %d", scanned)
	}
	// 2. 沙箱产物（服务日志）。
	filepath.Walk(cfg.WorkDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		b, _ := os.ReadFile(path)
		if strings.Contains(string(b), secretVal) {
			t.Fatalf("密钥明文出现在沙箱产物 %s", path)
		}
		return nil
	})
	// 3. 记录里只能出现 key@version 引用。
	data, err := os.ReadFile(filepath.Join(cfg.StateDir, "env-leak2.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"key": "tok"`) || !strings.Contains(string(data), `"version": 1`) {
		t.Fatalf("记录应只含密钥引用: %s", data)
	}
	t.Log("判定依据: 状态目录与沙箱产物均无密钥明文，记录仅含 tok@1 引用")
}
