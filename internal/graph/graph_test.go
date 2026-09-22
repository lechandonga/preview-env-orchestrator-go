package graph_test

import (
	"errors"
	"testing"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"
	"github.com/lechandonga/preview-env-orchestrator-go/internal/graph"
)

func TestTopoOrder(t *testing.T) {
	specs := []domain.ServiceSpec{
		{Name: "api", Image: "img", DependsOn: []string{"db", "cache"}},
		{Name: "web", Image: "img", DependsOn: []string{"api"}},
		{Name: "db", Image: "img"},
		{Name: "cache", Image: "img"},
	}
	order, err := graph.TopoOrder(specs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pos := map[string]int{}
	for i, n := range order {
		pos[n] = i
	}
	if !(pos["db"] < pos["api"] && pos["cache"] < pos["api"] && pos["api"] < pos["web"]) {
		t.Fatalf("dependency order violated: %v", order)
	}
	t.Logf("judgement: topo order = %v satisfies db,cache < api < web", order)
}

func TestTopoOrderCycle(t *testing.T) {
	specs := []domain.ServiceSpec{
		{Name: "a", Image: "i", DependsOn: []string{"c"}},
		{Name: "b", Image: "i", DependsOn: []string{"a"}},
		{Name: "c", Image: "i", DependsOn: []string{"b"}},
	}
	_, err := graph.TopoOrder(specs)
	var ce *domain.CycleError
	if !errors.As(err, &ce) {
		t.Fatalf("expected CycleError, got %T: %v", err, err)
	}
	if len(ce.Cycle) < 3 {
		t.Fatalf("cycle path too short: %v", ce.Cycle)
	}
	t.Logf("judgement: cycle rejected with path %v", ce.Cycle)
}

func TestTopoOrderUnknownDep(t *testing.T) {
	specs := []domain.ServiceSpec{
		{Name: "a", Image: "i", DependsOn: []string{"ghost"}},
	}
	_, err := graph.TopoOrder(specs)
	var ve *domain.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected ValidationError, got %T: %v", err, err)
	}
	t.Logf("judgement: unknown dependency rejected: %v", ve.Reasons)
}
