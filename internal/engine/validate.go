package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"
)

// validateSpec 对环境声明做纯静态校验，返回可区分的 ValidationError（原因列表）。
func validateSpec(spec domain.EnvironmentSpec) error {
	var reasons []string
	if spec.Name == "" {
		reasons = append(reasons, "environment name must not be empty")
	}
	if len(spec.Services) == 0 {
		reasons = append(reasons, "environment must declare at least one service")
	}
	names := map[string]bool{}
	portNames := map[string]map[string]bool{} // service -> port name 集合
	for _, svc := range spec.Services {
		if svc.Name == "" {
			reasons = append(reasons, "service name must not be empty")
			continue
		}
		if names[svc.Name] {
			reasons = append(reasons, fmt.Sprintf("duplicate service name %q", svc.Name))
		}
		names[svc.Name] = true
		if svc.Image == "" {
			reasons = append(reasons, fmt.Sprintf("service %q must declare an image", svc.Name))
		}
		if portNames[svc.Name] == nil {
			portNames[svc.Name] = map[string]bool{}
		}
		for _, p := range svc.Ports {
			if p.Name == "" {
				reasons = append(reasons, fmt.Sprintf("service %q has an unnamed port", svc.Name))
			} else if portNames[svc.Name][p.Name] {
				reasons = append(reasons, fmt.Sprintf("service %q declares duplicate port name %q", svc.Name, p.Name))
			}
			portNames[svc.Name][p.Name] = true
			if p.Fixed != 0 && (p.Fixed < 1 || p.Fixed > 65535) {
				reasons = append(reasons, fmt.Sprintf("service %q port %q out of range: %d", svc.Name, p.Name, p.Fixed))
			}
		}
		seenSecret := map[string]bool{}
		for _, r := range svc.Secrets {
			if r.Name == "" {
				reasons = append(reasons, fmt.Sprintf("service %q references an unnamed secret", svc.Name))
			} else if seenSecret[r.Name] {
				reasons = append(reasons, fmt.Sprintf("service %q references secret %q more than once", svc.Name, r.Name))
			}
			seenSecret[r.Name] = true
			if r.Version < 0 {
				reasons = append(reasons, fmt.Sprintf("service %q secret %q has negative version %d", svc.Name, r.Name, r.Version))
			}
		}
	}
	if len(reasons) > 0 {
		return &domain.ValidationError{Reasons: reasons}
	}
	return nil
}

// specHash 计算声明的规范化哈希：相同语义的描述（与切片顺序无关的部分）得到相同哈希，
// 从而“同一描述重复提交只得到同一个环境实例”。
func specHash(spec domain.EnvironmentSpec) string {
	h := sha256.New()
	fmt.Fprintf(h, "env=%s\n", spec.Name)
	fmt.Fprintf(h, "ttl=%d\n", int64(spec.TTL))

	svcs := append([]domain.ServiceSpec(nil), spec.Services...)
	sort.Slice(svcs, func(i, j int) bool { return svcs[i].Name < svcs[j].Name })
	for _, svc := range svcs {
		fmt.Fprintf(h, "svc=%s image=%s\n", svc.Name, svc.Image)

		deps := append([]string(nil), svc.DependsOn...)
		sort.Strings(deps)
		fmt.Fprintf(h, " deps=%v\n", deps)

		ports := append([]domain.PortSpec(nil), svc.Ports...)
		sort.Slice(ports, func(i, j int) bool { return ports[i].Name < ports[j].Name })
		for _, p := range ports {
			fmt.Fprintf(h, " port=%s fixed=%d\n", p.Name, p.Fixed)
		}

		secs := append([]domain.SecretRef(nil), svc.Secrets...)
		sort.Slice(secs, func(i, j int) bool {
			if secs[i].Name != secs[j].Name {
				return secs[i].Name < secs[j].Name
			}
			return secs[i].Version < secs[j].Version
		})
		for _, r := range secs {
			fmt.Fprintf(h, " secret=%s v%d\n", r.Name, r.Version)
		}

		envKeys := make([]string, 0, len(svc.Env))
		for k := range svc.Env {
			envKeys = append(envKeys, k)
		}
		sort.Strings(envKeys)
		for _, k := range envKeys {
			fmt.Fprintf(h, " env=%s=%s\n", k, svc.Env[k])
		}
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// checkConflict 比较新声明与既有实例。环境名相同即视为同一环境；
// 若哈希相同则天然幂等（调用方直接返回），否则按差异收敛（更新）。
// 保留该函数是为了将来拒绝不可变字段的冲突；目前 TTL 从有界改为永久属于允许的收敛。
func checkConflict(existing *domain.Environment, spec domain.EnvironmentSpec) error {
	if existing == nil {
		return nil
	}
	if specHash(spec) == existing.SpecHash {
		return nil
	}
	return nil
}
