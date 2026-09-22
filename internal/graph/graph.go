package graph

import (
	"fmt"
	"sort"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"
)

// TopoOrder 依据 DependsOn 返回服务名的拓扑启动顺序：
// 所有依赖（上游）都排在服务（下游）之前。
//
// 返回的错误情形可区分：
//   - 服务重名 / 依赖指向不存在的服务：普通 error，原因列表化；
//   - 依赖图成环：*domain.CycleError，附带环路径。
//
// 同层节点按名称排序，使结果在并发与重复提交下确定可解释。
func TopoOrder(services []domain.ServiceSpec) ([]string, error) {
	names := make(map[string]struct{}, len(services))
	var reasons []string
	for _, svc := range services {
		if svc.Name == "" {
			reasons = append(reasons, "service with empty name")
			continue
		}
		if _, dup := names[svc.Name]; dup {
			reasons = append(reasons, fmt.Sprintf("duplicate service name %q", svc.Name))
		}
		names[svc.Name] = struct{}{}
	}
	if reasons != nil {
		return nil, &domain.ValidationError{Reasons: reasons}
	}

	// indeg[u] = u 有多少个依赖；依赖边 dep -> svc。
	indeg := make(map[string]int, len(services))
	dependents := make(map[string][]string, len(services))
	for _, svc := range services {
		indeg[svc.Name] = 0
	}
	for _, svc := range services {
		seen := map[string]bool{}
		for _, dep := range svc.DependsOn {
			if _, ok := names[dep]; !ok {
				reasons = append(reasons, fmt.Sprintf("service %q depends on unknown service %q", svc.Name, dep))
				continue
			}
			if dep == svc.Name {
				reasons = append(reasons, fmt.Sprintf("service %q depends on itself", svc.Name))
				continue
			}
			if seen[dep] {
				// 重复声明依赖不影响拓扑，忽略。
				continue
			}
			seen[dep] = true
			indeg[svc.Name]++
			dependents[dep] = append(dependents[dep], svc.Name)
		}
	}
	if reasons != nil {
		return nil, &domain.ValidationError{Reasons: reasons}
	}

	// Kahn：每层取全部入度为 0 的节点，排序保证确定性。
	ready := make([]string, 0)
	for name, d := range indeg {
		if d == 0 {
			ready = append(ready, name)
		}
	}
	order := make([]string, 0, len(services))
	for len(ready) > 0 {
		sort.Strings(ready)
		next := make([]string, 0)
		for _, name := range ready {
			order = append(order, name)
			for _, d := range dependents[name] {
				indeg[d]--
				if indeg[d] == 0 {
					next = append(next, d)
				}
			}
		}
		ready = next
	}

	if len(order) != len(services) {
		// 剩余节点都在某个环上；用 DFS 抽出一条具体环路径供错误展示。
		remaining := map[string]bool{}
		for name, d := range indeg {
			if d > 0 {
				remaining[name] = true
			}
		}
		return nil, &domain.CycleError{Cycle: findCycle(services, remaining)}
	}
	return order, nil
}

// findCycle 在仍有入度的节点集合中沿依赖边回溯出一条环。
func findCycle(services []domain.ServiceSpec, nodes map[string]bool) []string {
	deps := map[string][]string{}
	for _, svc := range services {
		if nodes[svc.Name] {
			deps[svc.Name] = svc.DependsOn
		}
	}

	color := map[string]int{} // 0=未访问,1=在栈上,2=完成
	var stack []string
	var cycle []string

	var dfs func(string) bool
	dfs = func(u string) bool {
		color[u] = 1
		stack = append(stack, u)
		// 排序使报告的环路径稳定。
		ds := append([]string(nil), deps[u]...)
		sort.Strings(ds)
		for _, v := range ds {
			if !nodes[v] {
				continue
			}
			switch color[v] {
			case 1:
				// 从栈中 v 首次出现的位置截取到当前，得到环。
				start := 0
				for i, n := range stack {
					if n == v {
						start = i
						break
					}
				}
				cycle = append([]string(nil), stack[start:]...)
				cycle = append(cycle, v)
				return true
			case 0:
				if dfs(v) {
					return true
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[u] = 2
		return false
	}

	startNodes := make([]string, 0, len(nodes))
	for n := range nodes {
		startNodes = append(startNodes, n)
	}
	sort.Strings(startNodes)
	for _, n := range startNodes {
		if color[n] == 0 && dfs(n) {
			return cycle
		}
	}
	// 理论上不可达：存在入度>0 的节点却没找到环。
	return []string{"<unknown>"}
}
