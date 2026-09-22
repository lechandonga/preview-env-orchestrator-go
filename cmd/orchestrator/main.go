// Command orchestrator 是本地预览环境编排器的端到端演示入口：
// 在临时状态目录中创建一个带依赖、端口、密钥引用与 TTL 的环境，
// 演示幂等、访问、对账与销毁的确定性结果（不输出任何密钥值）。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"
	"github.com/lechandonga/preview-env-orchestrator-go/internal/engine"
	"github.com/lechandonga/preview-env-orchestrator-go/internal/secret"
)

func main() {
	ctx := context.Background()
	stateDir := filepath.Join(os.TempDir(), "preview-orchestrator-demo")
	if err := os.RemoveAll(stateDir); err != nil {
		log.Fatal(err)
	}

	secrets := secret.NewStore()
	dbVer, err := secrets.Put("db-password", "demo-secret-value")
	must(err)

	e, err := engine.NewAndRecover(engine.Config{
		StateDir:        stateDir,
		MaxEnvironments: 4,
		PortMin:         28000,
		PortMax:         28100,
		LimitPolicy:     engine.PolicyReject,
		SecretStore:     secrets,
	})
	must(err)
	e.SetLogger(func(format string, args ...any) {
		log.Printf("[flow] "+format, args...)
	})

	spec := domain.EnvironmentSpec{
		Name: "demo",
		TTL:  2 * time.Second,
		Services: []domain.ServiceSpec{
			{Name: "db", Image: "postgres", Ports: []domain.PortSpec{{Name: "pg"}},
				Secrets: []domain.SecretRef{{Name: "db-password"}}},
			{Name: "api", Image: "svc", DependsOn: []string{"db"}, Ports: []domain.PortSpec{{Name: "http"}}},
			{Name: "web", Image: "web", DependsOn: []string{"api"}},
		},
	}

	fmt.Println("== 第一次 Apply ==")
	env, err := e.Apply(ctx, spec)
	must(err)
	printEnv(env)

	fmt.Println("== 第二次相同 Apply（必须幂等：同代际、同 PID）==")
	again, err := e.Apply(ctx, spec)
	must(err)
	fmt.Printf("generation=%d dbPID=%d (前后一致=%v)\n",
		again.Generation, again.Services["db"].PID, again.Services["db"].PID == env.Services["db"].PID)

	fmt.Printf("== 密钥轮换（当前已到 v%d），既有环境仍绑定 v%d ==\n", dbVer, dbVer)
	newVer, err := secrets.Rotate("db-password", "rotated-secret-value")
	must(err)
	if rec, err := e.Reconcile(ctx, "demo"); err != nil {
		log.Fatal(err)
	} else {
		fmt.Printf("existing demo bound secret version=%d (期望 1，轮换不影响已绑定环境)\n",
			rec.Services["db"].Secrets[0].Version)
	}
	_ = newVer

	fmt.Println("== 等待 TTL 到期并触发 ReconcileAll 自动回收 ==")
	time.Sleep(2200 * time.Millisecond)
	must(e.ReconcileAll(ctx, time.Now()))
	if _, ok := e.Get("demo"); ok {
		log.Fatal("过期环境应当已被回收")
	}
	fmt.Println("demo 已到期回收；再次 Delete 返回明确 NotFound：")
	if err := e.Delete(ctx, "demo"); err != nil {
		fmt.Printf("Delete -> %v\n", err)
	}
	fmt.Println("OK: 端到端演示完成，密钥值从未被打印。")
}

func printEnv(env *domain.Environment) {
	fmt.Printf("environment=%s phase=%s generation=%d expiresAt=%s\n",
		env.Name, env.Phase, env.Generation,
		func() string {
			if env.ExpiresAt.IsZero() {
				return "-"
			}
			return env.ExpiresAt.Format(time.RFC3339)
		}())
	for name, st := range env.Services {
		ports := ""
		for _, p := range st.Ports {
			ports += fmt.Sprintf(" %s=%d", p.Name, p.Port)
		}
		fmt.Printf("  service=%s phase=%s pid=%d%s\n", name, st.Phase, st.PID, ports)
	}
}

func must(err error) {
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
}
