package spec

import (
	"errors"
	"testing"
)

func validSpec() *EnvSpec {
	return &EnvSpec{
		Name: "e1",
		Services: []ServiceSpec{
			{Name: "db", Command: []string{"sleep", "30"}},
			{Name: "api", Command: []string{"sleep", "30"}, DependsOn: []string{"db"}},
		},
	}
}

func TestValidateOK(t *testing.T) {
	if err := Validate(validSpec()); err != nil {
		t.Fatalf("valid spec rejected: %v", err)
	}
}

func TestValidateCycle(t *testing.T) {
	s := &EnvSpec{Name: "e", Services: []ServiceSpec{
		{Name: "a", Command: []string{"x"}, DependsOn: []string{"b"}},
		{Name: "b", Command: []string{"x"}, DependsOn: []string{"a"}},
	}}
	err := Validate(s)
	var ce *CycleError
	if !errors.As(err, &ce) {
		t.Fatalf("expected CycleError, got %v", err)
	}
	t.Logf("判定依据: 环路径 = %v", ce.Path)
}

func TestValidateBasics(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*EnvSpec)
	}{
		{"empty env name", func(s *EnvSpec) { s.Name = "" }},
		{"no services", func(s *EnvSpec) { s.Services = nil }},
		{"dup service", func(s *EnvSpec) { s.Services = append(s.Services, s.Services[0]) }},
		{"empty command", func(s *EnvSpec) { s.Services[0].Command = nil }},
		{"unknown dep", func(s *EnvSpec) { s.Services[0].DependsOn = []string{"ghost"} }},
		{"self dep", func(s *EnvSpec) { s.Services[0].DependsOn = []string{"db"} }},
		{"bad secret ref", func(s *EnvSpec) { s.Services[0].SecretRefs = []SecretRef{{Key: "k"}} }},
	}
	for _, c := range cases {
		s := validSpec()
		c.mut(s)
		if err := Validate(s); err == nil {
			t.Fatalf("%s: expected rejection", c.name)
		} else {
			t.Logf("%s 被拒绝: %v", c.name, err)
		}
	}
}

func TestKeyStableAndOrderIndependent(t *testing.T) {
	a := validSpec()
	b := validSpec()
	b.Services = []ServiceSpec{b.Services[1], b.Services[0]} // 交换顺序
	if a.Key() != b.Key() {
		t.Fatalf("key should be order-independent: %s vs %s", a.Key(), b.Key())
	}
	b.Services[0].Env = map[string]string{"X": "1"}
	if a.Key() == b.Key() {
		t.Fatalf("key should change with content")
	}
}

func TestTopoOrder(t *testing.T) {
	order, err := TopoOrder(validSpec())
	if err != nil {
		t.Fatal(err)
	}
	if order[0] != "db" || order[1] != "api" {
		t.Fatalf("bad order: %v", order)
	}
}
