package engine_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"
	"github.com/lechandonga/preview-env-orchestrator-go/internal/engine"
	"github.com/lechandonga/preview-env-orchestrator-go/internal/secret"
)

type testHarness struct {
	dir    string
	store  *secret.Store
	engine *engine.Engine
	t      *testing.T
	logMu  sync.Mutex
}

func newHarness(t *testing.T, maxEnvs, portMin, portMax int, policy engine.LimitPolicy) *testHarness {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "state")
	ss := secret.NewStore()
	e, err := engine.New(engine.Config{
		StateDir:        dir,
		MaxEnvironments: maxEnvs,
		PortMin:         portMin,
		PortMax:         portMax,
		LimitPolicy:     policy,
		SecretStore:     ss,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	h := &testHarness{dir: dir, store: ss, engine: e, t: t}
	e.SetLogger(h.log)
	return h
}

func (h *testHarness) log(format string, args ...any) {
	h.logMu.Lock()
	defer h.logMu.Unlock()
	h.t.Logf("[flow] "+format, args...)
}

// basicSpec: db <- api <- web 的三服务环境。
func basicSpec(name string) domain.EnvironmentSpec {
	return domain.EnvironmentSpec{
		Name: name,
		Services: []domain.ServiceSpec{
			{Name: "db", Image: "postgres", Ports: []domain.PortSpec{{Name: "pg", Fixed: 0}}},
			{Name: "api", Image: "svc", DependsOn: []string{"db"}, Ports: []domain.PortSpec{{Name: "http"}}},
			{Name: "web", Image: "web", DependsOn: []string{"api"}},
		},
	}
}

func (h *testHarness) apply(spec domain.EnvironmentSpec) *domain.Environment {
	h.t.Helper()
	env, err := h.engine.Apply(context.Background(), spec)
	if err != nil {
		h.t.Fatalf("Apply(%s): %v", spec.Name, err)
	}
	return env
}

func (h *testHarness) assertPhase(name string, want domain.EnvironmentPhase) *domain.Environment {
	h.t.Helper()
	env, ok := h.engine.Get(name)
	if !ok {
		h.t.Fatalf("environment %q not accessible", name)
	}
	if env.Phase != want {
		h.t.Fatalf("environment %q phase = %s, want %s; services=%s", name, env.Phase, want, serviceSummary(env))
	}
	return env
}

func serviceSummary(env *domain.Environment) string {
	s := ""
	for name, st := range env.Services {
		s += fmt.Sprintf("%s=%s(%s) ", name, st.Phase, st.Reason)
	}
	return s
}
