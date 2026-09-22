// Package spec 定义声明式环境描述及其校验。
package spec

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"time"
)

// SecretRef 表示对一个密钥的引用（只能引用，不允许内联明文）。
type SecretRef struct {
	Key     string // 密钥名
	Version int    // 0 表示绑定时解析为当前最新版本
	Env     string // 注入到服务的环境变量名
}

// PortRequest 表示一个独占端口申请。
type PortRequest struct {
	Name      string
	Preferred int // 0 表示自动分配
}

// ServiceSpec 描述一个服务。
type ServiceSpec struct {
	Name       string
	Command    []string
	DependsOn  []string
	Ports      []PortRequest
	Env        map[string]string
	SecretRefs []SecretRef
}

// EnvSpec 是环境声明式描述。
type EnvSpec struct {
	Name     string
	TTL      time.Duration // 0 表示不过期
	Services []ServiceSpec
}

// ServiceKey 返回单个服务的指纹（更新时做服务级 diff 用）。
func ServiceKey(svc *ServiceSpec) string {
	h := sha256.New()
	io.WriteString(h, "|svc="+svc.Name)
	io.WriteString(h, "|cmd=")
	for _, c := range svc.Command {
		io.WriteString(h, c+"\x00")
	}
	deps := append([]string(nil), svc.DependsOn...)
	sort.Strings(deps)
	for _, d := range deps {
		io.WriteString(h, "|dep="+d)
	}
	ports := append([]PortRequest(nil), svc.Ports...)
	sort.Slice(ports, func(i, j int) bool { return ports[i].Name < ports[j].Name })
	for _, p := range ports {
		fmt.Fprintf(h, "|port=%s:%d", p.Name, p.Preferred)
	}
	envKeys := make([]string, 0, len(svc.Env))
	for k := range svc.Env {
		envKeys = append(envKeys, k)
	}
	sort.Strings(envKeys)
	for _, k := range envKeys {
		fmt.Fprintf(h, "|env=%s=%s", k, svc.Env[k])
	}
	refs := append([]SecretRef(nil), svc.SecretRefs...)
	sort.Slice(refs, func(i, j int) bool { return refs[i].Env < refs[j].Env })
	for _, r := range refs {
		fmt.Fprintf(h, "|secret=%s@%d->%s", r.Key, r.Version, r.Env)
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// Key 返回描述的幂等键（内容指纹）。同一描述 Key 相同。
func (s *EnvSpec) Key() string {
	h := sha256.New()
	io.WriteString(h, s.Name)
	fmt.Fprintf(h, "|ttl=%d", s.TTL)
	svcs := make([]ServiceSpec, len(s.Services))
	copy(svcs, s.Services)
	sort.Slice(svcs, func(i, j int) bool { return svcs[i].Name < svcs[j].Name })
	for i := range svcs {
		io.WriteString(h, "|"+ServiceKey(&svcs[i]))
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// CycleError 表示依赖图中存在环。
type CycleError struct{ Path []string }

func (e *CycleError) Error() string { return fmt.Sprintf("dependency cycle: %v", e.Path) }

// ValidationError 表示描述非法，调用方可与冲突、容量等原因区分。
type ValidationError struct{ Reason string }

func (e *ValidationError) Error() string { return "invalid spec: " + e.Reason }

// Validate 校验描述合法性（含依赖环检测）。
func Validate(s *EnvSpec) error {
	if s.Name == "" {
		return &ValidationError{Reason: "environment name is empty"}
	}
	if len(s.Services) == 0 {
		return &ValidationError{Reason: "no services defined"}
	}
	seen := map[string]bool{}
	for _, svc := range s.Services {
		if svc.Name == "" {
			return &ValidationError{Reason: "service name is empty"}
		}
		if seen[svc.Name] {
			return &ValidationError{Reason: "duplicate service name: " + svc.Name}
		}
		seen[svc.Name] = true
		if len(svc.Command) == 0 {
			return &ValidationError{Reason: "service " + svc.Name + " has empty command"}
		}
		portNames := map[string]bool{}
		for _, p := range svc.Ports {
			if p.Name == "" {
				return &ValidationError{Reason: "service " + svc.Name + " has unnamed port"}
			}
			if portNames[p.Name] {
				return &ValidationError{Reason: "service " + svc.Name + " duplicate port name " + p.Name}
			}
			portNames[p.Name] = true
			if p.Preferred < 0 || p.Preferred > 65535 {
				return &ValidationError{Reason: fmt.Sprintf("service %s port %s: invalid preferred port %d", svc.Name, p.Name, p.Preferred)}
			}
		}
		for _, r := range svc.SecretRefs {
			if r.Key == "" || r.Env == "" {
				return &ValidationError{Reason: "service " + svc.Name + " has incomplete secret reference"}
			}
			if r.Version < 0 {
				return &ValidationError{Reason: "service " + svc.Name + " has negative secret version"}
			}
		}
	}
	// 依赖必须指向已声明服务。
	for _, svc := range s.Services {
		for _, d := range svc.DependsOn {
			if !seen[d] {
				return &ValidationError{Reason: fmt.Sprintf("service %s depends on unknown service %s", svc.Name, d)}
			}
			if d == svc.Name {
				return &ValidationError{Reason: "service " + svc.Name + " depends on itself"}
			}
		}
	}
	// 环检测（DFS 三色标记）。
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := map[string]int{}
	var stack []string
	var visit func(n string) error
	visit = func(n string) error {
		color[n] = gray
		stack = append(stack, n)
		for _, svc := range s.Services {
			if svc.Name != n {
				continue
			}
			for _, d := range svc.DependsOn {
				switch color[d] {
				case gray:
					// 找到环：截取栈中从 d 开始的部分。
					idx := 0
					for i, x := range stack {
						if x == d {
							idx = i
						}
					}
					return &CycleError{Path: append(append([]string(nil), stack[idx:]...), d)}
				case white:
					if err := visit(d); err != nil {
						return err
					}
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[n] = black
		return nil
	}
	for _, svc := range s.Services {
		if color[svc.Name] == white {
			if err := visit(svc.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

// TopoOrder 返回拓扑序（上游在前）。调用前需先通过 Validate。
func TopoOrder(s *EnvSpec) ([]string, error) {
	indeg := map[string]int{}
	for _, svc := range s.Services {
		indeg[svc.Name] = len(svc.DependsOn)
	}
	var order []string
	// 确定性：按名称排序处理。
	ready := []string{}
	for _, svc := range s.Services {
		if indeg[svc.Name] == 0 {
			ready = append(ready, svc.Name)
		}
	}
	sort.Strings(ready)
	for len(ready) > 0 {
		n := ready[0]
		ready = ready[1:]
		order = append(order, n)
		var next []string
		for _, svc := range s.Services {
			for _, d := range svc.DependsOn {
				if d == n {
					indeg[svc.Name]--
					if indeg[svc.Name] == 0 {
						next = append(next, svc.Name)
					}
				}
			}
		}
		sort.Strings(next)
		ready = append(ready, next...)
	}
	if len(order) != len(s.Services) {
		return nil, &CycleError{Path: []string{"?"}}
	}
	return order, nil
}
