package orchestrator

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lechandonga/preview-env-orchestrator-go/secrets"
	"github.com/lechandonga/preview-env-orchestrator-go/spec"
)

func newTestOrch(t *testing.T, mutate func(*Config)) (*Orchestrator, *secrets.Store) {
	t.Helper()
	cfg := Config{
		MaxEnvs:      4,
		PortMin:      31000,
		PortMax:      31100,
		WorkDir:      t.TempDir(),
		ReadyTimeout: 60 * time.Millisecond,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	sec := secrets.NewStore()
	o, err := New(cfg, sec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o.Close)
	return o, sec
}

func svc(name string, deps ...string) spec.ServiceSpec {
	return spec.ServiceSpec{Name: name, Command: []string{"sleep", "30"}, DependsOn: deps}
}

func dumpLedger(t *testing.T, o *Orchestrator) {
	t.Helper()
	t.Log("---- 资源分配/释放流水 ----")
	for _, e := range o.Ledger().Events() {
		t.Log(e.String())
	}
}

func TestApplyIdempotent(t *testing.T) {
	o, _ := newTestOrch(t, nil)
	s := &spec.EnvSpec{Name: "e1", Services: []spec.ServiceSpec{
		func() spec.ServiceSpec { x := svc("web"); x.Ports = []spec.PortRequest{{Name: "http"}}; return x }(),
	}}
	e1, err := o.Apply(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	e2, err := o.Apply(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if e1.SpecKey != e2.SpecKey || e1.Services["web"].Ports["http"] != e2.Services["web"].Ports["http"] {
		t.Fatalf("重复提交未得到同一实例: %+v vs %+v", e1, e2)
	}
	if n := o.Ledger().Count("alloc-port"); n != 1 {
		t.Fatalf("幂等提交不应追加副作用, alloc-port=%d", n)
	}
	t.Logf("判定依据: 两次 Apply 返回 SpecKey=%s 端口=%d, alloc-port 事件仅 1 次", e1.SpecKey, e1.Services["web"].Ports["http"])
	dumpLedger(t, o)
}

func TestApplyInvalidAndCycle(t *testing.T) {
	o, _ := newTestOrch(t, nil)
	bad := &spec.EnvSpec{Name: "", Services: []spec.ServiceSpec{svc("a")}}
	if _, err := o.Apply(context.Background(), bad); !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("expected ErrInvalidSpec, got %v", err)
	}
	cyc := &spec.EnvSpec{Name: "c", Services: []spec.ServiceSpec{
		{Name: "a", Command: []string{"sleep", "30"}, DependsOn: []string{"b"}},
		{Name: "b", Command: []string{"sleep", "30"}, DependsOn: []string{"a"}},
	}}
	if _, err := o.Apply(context.Background(), cyc); !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("expected ErrDependencyCycle, got %v", err)
	}
	t.Log("判定依据: 非法描述与依赖环分别被 ErrInvalidSpec / ErrDependencyCycle 拒绝")
}

func TestDependencyFailureBlocksDownstream(t *testing.T) {
	o, _ := newTestOrch(t, nil)
	s := &spec.EnvSpec{Name: "dep", Services: []spec.ServiceSpec{
		{Name: "db", Command: []string{"false"}}, // 立即退出 = 启动失败
		svc("api", "db"),
		svc("worker", "api"),
		svc("sidecar"), // 无依赖，应正常启动
	}}
	env, err := o.Apply(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if env.Status != StatusFailed {
		t.Fatalf("status=%s, want Failed", env.Status)
	}
	if !env.Services["db"].Failed {
		t.Fatal("db 应标记为失败")
	}
	for _, n := range []string{"api", "worker"} {
		if !env.Services[n].Blocked {
			t.Fatalf("%s 应被标记为阻塞", n)
		}
		if env.Services[n].Running {
			t.Fatalf("%s 不应被启动", n)
		}
	}
	if !env.Services["sidecar"].Running {
		t.Fatal("无依赖分支 sidecar 应正常启动")
	}
	t.Logf("判定依据: db=Failed, api/worker=Blocked 且未启动, sidecar=Running, env=%s", env.Status)
	dumpLedger(t, o)
}

func TestSecretMissingFailsExplicitly(t *testing.T) {
	o, _ := newTestOrch(t, nil)
	s := &spec.EnvSpec{Name: "sec", Services: []spec.ServiceSpec{
		func() spec.ServiceSpec {
			x := svc("web")
			x.Ports = []spec.PortRequest{{Name: "http"}}
			x.SecretRefs = []spec.SecretRef{{Key: "ghost", Env: "TOKEN"}}
			return x
		}(),
	}}
	_, err := o.Apply(context.Background(), s)
	if !errors.Is(err, ErrSecretRef) {
		t.Fatalf("expected ErrSecretRef, got %v", err)
	}
	if n := o.Ports().InUse(); n != 0 {
		t.Fatalf("失败后端口必须回滚释放, 残留=%d", n)
	}
	if got := o.List(); len(got) != 0 {
		t.Fatalf("失败创建不应留下环境: %v", got)
	}
	t.Log("判定依据: 缺失密钥 -> ErrSecretRef; 端口全部回滚, 无环境残留")
	dumpLedger(t, o)
}

func TestSecretRotationBinding(t *testing.T) {
	o, sec := newTestOrch(t, nil)
	sec.Put("db-pass", "v1-secret")
	mk := func(name string) *spec.EnvSpec {
		x := svc("web")
		x.SecretRefs = []spec.SecretRef{{Key: "db-pass", Env: "DB_PASS"}} // version=0 绑定时解析
		return &spec.EnvSpec{Name: name, Services: []spec.ServiceSpec{x}}
	}
	if _, err := o.Apply(context.Background(), mk("old")); err != nil {
		t.Fatal(err)
	}
	if _, err := sec.Rotate("db-pass", "v2-secret"); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Apply(context.Background(), mk("new")); err != nil {
		t.Fatal(err)
	}
	oldB, _ := o.SecretBindings("old", "web")
	newB, _ := o.SecretBindings("new", "web")
	if oldB["DB_PASS"] != 1 || newB["DB_PASS"] != 2 {
		t.Fatalf("轮换语义错误: old=%v new=%v", oldB, newB)
	}
	t.Logf("判定依据: 已存在环境绑定 v%d, 轮换后新环境绑定 v%d", oldB["DB_PASS"], newB["DB_PASS"])
}

func TestSecretVersionInvalid(t *testing.T) {
	o, sec := newTestOrch(t, nil)
	sec.Put("k", "v1")
	x := svc("web")
	x.SecretRefs = []spec.SecretRef{{Key: "k", Version: 9, Env: "K"}}
	_, err := o.Apply(context.Background(), &spec.EnvSpec{Name: "e", Services: []spec.ServiceSpec{x}})
	if !errors.Is(err, ErrSecretRef) {
		t.Fatalf("失效版本必须明确失败, got %v", err)
	}
	t.Logf("判定依据: 版本失效 -> %v", err)
}

func TestSecretNeverLeaks(t *testing.T) {
	o, sec := newTestOrch(t, nil)
	const secretVal = "super-secret-value-xyz"
	sec.Put("tok", secretVal)
	x := svc("web")
	x.SecretRefs = []spec.SecretRef{{Key: "tok", Env: "TOK"}}
	if _, err := o.Apply(context.Background(), &spec.EnvSpec{Name: "leak", Services: []spec.ServiceSpec{x}}); err != nil {
		t.Fatal(err)
	}
	for _, e := range o.Ledger().Events() {
		if strings.Contains(e.Detail, secretVal) {
			t.Fatalf("密钥出现在流水中: %v", e)
		}
	}
	// 检查沙箱产物（日志文件等）不含密钥。
	entries, _ := os.ReadDir(o.cfg.WorkDir)
	for _, ent := range entries {
		data, _ := os.ReadDir(o.cfg.WorkDir + "/" + ent.Name())
		for _, f := range data {
			b, _ := os.ReadFile(o.cfg.WorkDir + "/" + ent.Name() + "/" + f.Name())
			if strings.Contains(string(b), secretVal) {
				t.Fatalf("密钥出现在产物文件 %s/%s", ent.Name(), f.Name())
			}
		}
	}
	t.Log("判定依据: 流水与沙箱产物中均不含密钥明文")
}

func TestCapacityRejectAndQueue(t *testing.T) {
	o, _ := newTestOrch(t, func(c *Config) { c.MaxEnvs = 1 })
	mk := func(n string) *spec.EnvSpec { return &spec.EnvSpec{Name: n, Services: []spec.ServiceSpec{svc("web")}} }
	if _, err := o.Apply(context.Background(), mk("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Apply(context.Background(), mk("b")); !errors.Is(err, ErrCapacity) {
		t.Fatalf("达到上限应拒绝, got %v", err)
	}
	t.Log("判定依据: MaxEnvs=1 时第二个环境被 ErrCapacity 拒绝")

	// 排队策略：阻塞直到槽位释放。
	o2, _ := newTestOrch(t, func(c *Config) { c.MaxEnvs = 1; c.Queue = true })
	if _, err := o2.Apply(context.Background(), mk("a")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := o2.Apply(ctx, mk("b")); !errors.Is(err, ErrCapacity) {
		t.Fatalf("排队超时应失败, got %v", err)
	}
	go func() { time.Sleep(50 * time.Millisecond); o2.Destroy("a") }()
	if _, err := o2.Apply(context.Background(), mk("b")); err != nil {
		t.Fatalf("槽位释放后排队应成功, got %v", err)
	}
	t.Log("判定依据: Queue=true 时排队等待, 槽位释放后创建成功")
}

func TestPortConflictReallocAndExhaustion(t *testing.T) {
	o, _ := newTestOrch(t, func(c *Config) { c.PortMin, c.PortMax = 32000, 32001 })
	mk := func(n string, preferred int) *spec.EnvSpec {
		x := svc("web")
		x.Ports = []spec.PortRequest{{Name: "http", Preferred: preferred}}
		return &spec.EnvSpec{Name: n, Services: []spec.ServiceSpec{x}}
	}
	e1, err := o.Apply(context.Background(), mk("p1", 32000))
	if err != nil {
		t.Fatal(err)
	}
	e2, err := o.Apply(context.Background(), mk("p2", 32000)) // 冲突 -> 重分配
	if err != nil {
		t.Fatal(err)
	}
	p1, p2 := e1.Services["web"].Ports["http"], e2.Services["web"].Ports["http"]
	if p1 != 32000 || p2 == 32000 {
		t.Fatalf("冲突应重分配: p1=%d p2=%d", p1, p2)
	}
	// 池耗尽（只有 2 个端口）。
	if _, err := o.Apply(context.Background(), mk("p3", 0)); !errors.Is(err, ErrCapacity) {
		t.Fatalf("端口耗尽应拒绝, got %v", err)
	}
	if got := o.List(); len(got) != 2 {
		t.Fatalf("耗尽失败不应残留环境: %v", got)
	}
	t.Logf("判定依据: p1=%d 占用首选, p2=%d 冲突后重分配, p3 因池耗尽被 ErrCapacity 拒绝", p1, p2)
	dumpLedger(t, o)
}

func TestUpdateConverges(t *testing.T) {
	o, _ := newTestOrch(t, nil)
	v1 := &spec.EnvSpec{Name: "u", Services: []spec.ServiceSpec{
		func() spec.ServiceSpec { x := svc("keep"); x.Ports = []spec.PortRequest{{Name: "http"}}; return x }(),
		svc("drop"),
	}}
	e1, err := o.Apply(context.Background(), v1)
	if err != nil {
		t.Fatal(err)
	}
	keepPort := e1.Services["keep"].Ports["http"]

	v2 := &spec.EnvSpec{Name: "u", Services: []spec.ServiceSpec{
		func() spec.ServiceSpec { x := svc("keep"); x.Ports = []spec.PortRequest{{Name: "http"}}; return x }(),
		svc("added"),
	}}
	e2, err := o.Apply(context.Background(), v2)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e2.Services["drop"]; ok {
		t.Fatal("被移除的服务不应存在")
	}
	if !e2.Services["added"].Running {
		t.Fatal("新增服务应启动")
	}
	if e2.Services["keep"].Ports["http"] != keepPort {
		t.Fatal("未变化服务的端口不应被重新分配")
	}
	// 重复提交 v2 不应追加副作用。
	before := len(o.Ledger().Events())
	if _, err := o.Apply(context.Background(), v2); err != nil {
		t.Fatal(err)
	}
	if len(o.Ledger().Events()) != before {
		t.Fatal("重复提交相同描述不应产生新副作用")
	}
	t.Logf("判定依据: drop 被回收, added 被启动, keep 端口=%d 保持不变, 重复提交零副作用", keepPort)
	dumpLedger(t, o)
}

func TestGetNotFound(t *testing.T) {
	o, _ := newTestOrch(t, nil)
	if _, err := o.Get("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}
