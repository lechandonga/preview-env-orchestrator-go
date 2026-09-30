package orchestrator

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/lechandonga/preview-env-orchestrator-go/spec"
)

// TestHelperProcess 不是真实测试：被 recovery 测试以子进程方式启动，
// 创建一个环境后打印 READY 并阻塞，等待父进程 SIGKILL（模拟引擎被强杀）。
// 服务进程是独立进程组，引擎被杀后仍存活，等待新实例接管。
func TestHelperProcess(t *testing.T) {
	if os.Getenv("HELPER") != "1" {
		return
	}
	cfg := Config{
		MaxEnvs:      4,
		PortMin:      33000,
		PortMax:      33200,
		WorkDir:      os.Getenv("HELPER_WORKDIR"),
		StateDir:     os.Getenv("HELPER_STATEDIR"),
		ReadyTimeout: 60 * time.Millisecond,
	}
	o, err := New(cfg, secretStoreWithToken())
	if err != nil {
		t.Fatal(err)
	}
	web := svc("web")
	web.Ports = []spec.PortRequest{{Name: "http", Preferred: 33120}}
	s := &spec.EnvSpec{Name: "hk", Services: []spec.ServiceSpec{web}}
	if _, err := o.Apply(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	os.Stdout.WriteString("READY\n")
	select {} // 等待 SIGKILL
}
