package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"
	"github.com/lechandonga/preview-env-orchestrator-go/internal/graph"
	"github.com/lechandonga/preview-env-orchestrator-go/internal/resources"
)

// Apply 以幂等方式创建或更新环境。
//
// 语义：
//   - 相同声明（specHash 相同）重复提交 -> 返回同一个环境实例，无额外副作用；
//   - 不同声明 -> 收敛（更新）：当前实现采用“受控重建”——先停旧服务释放资源，
//     再按新描述顺序启动，更新期间实例处于 Creating，访问方拿不到半成品；
//   - 声明非法（ValidationError/CycleError）、密钥缺失（SecretError）、
//     配额满（QuotaExceededError）在任何资源分配之前被拒绝；
//   - 正向步骤失败时回滚全部已分配资源，回滚失败返回 RollbackError（可重试）。
func (e *Engine) Apply(ctx context.Context, spec domain.EnvironmentSpec) (*domain.Environment, error) {
	if err := validateSpec(spec); err != nil {
		return nil, err
	}
	if _, err := graph.TopoOrder(spec.Services); err != nil {
		return nil, err // ValidationError 或 CycleError
	}
	// 更新路径上若探测到“刚被删除”的竞争，最多重试一轮（重走创建门控）。
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		env, err := e.applyOnce(ctx, spec)
		if !errorsIs(err, errRetryCreate) {
			return env, err
		}
		lastErr = err
	}
	return nil, lastErr
}

func (e *Engine) applyOnce(ctx context.Context, spec domain.EnvironmentSpec) (*domain.Environment, error) {
	order, err := graph.TopoOrder(spec.Services)
	if err != nil {
		return nil, err
	}
	hash := specHash(spec)

	// 阶段 A（不持环境锁）：密钥引用全部解析成功才允许排队/分配资源，
	// fail-fast 且不泄漏密钥值。排队等待配额期间也不持有环境锁，
	// 因而不会阻塞同一环境上的 Get/Delete（访问者永不被排队饿死）。
	resolved := map[string]map[string]string{}
	bindingsBySvc := map[string][]domain.SecretBinding{}
	for _, svc := range spec.Services {
		vals, binds, rerr := e.resolveSecretRefs(spec.Name, svc.Name, svc.Secrets)
		if rerr != nil {
			return nil, rerr
		}
		resolved[svc.Name] = vals
		bindingsBySvc[svc.Name] = binds
	}

	// 无锁探测存在性，决定走创建门控还是更新路径。
	_, peekOK, err := e.store.Get(spec.Name)
	if err != nil {
		return nil, err
	}
	if !peekOK {
		// 创建去重：同环境并发创建只有一个领队真正申请名额/执行创建，
		// 跟随者等待并复用领队结果，避免 N 个请求抢占 N 个名额。
		g, leader := e.beginCreate(spec.Name)
		if !leader {
			select {
			case <-g.done:
				return g.env, g.err
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		env, cerr := e.createLocked(ctx, spec, order, hash, resolved, bindingsBySvc)
		e.endCreate(spec.Name, g, env, cerr)
		return env, cerr
	}

	// 更新路径：环境已存在，直接持锁串行化（不产生新名额）。
	b := newResourceBundle()
	fl, unlock := e.lockEnvFully(spec.Name)
	defer unlock()
	_ = fl

	existing, ok, err := e.store.Get(spec.Name)
	if err != nil {
		return nil, err
	}
	if !ok {
		// 探测时环境存在，持锁后却消失（期间被删除）：交还外层重走创建门控。
		return nil, errRetryCreate
	}
	if ok && existing.Phase == domain.PhaseDeleting {
		return nil, &domain.ConflictError{Env: spec.Name, Reason: "environment is being deleted"}
	}
	if ok && existing.SpecHash == hash && (existing.Phase == domain.PhaseReady || existing.Phase == domain.PhaseDegraded) {
		e.logf("apply: env=%s unchanged hash=%s phase=%s -> idempotent no-op", spec.Name, hash, existing.Phase)
		return existing.Clone(), nil
	}
	if err := checkConflict(existing, spec); err != nil {
		return nil, err
	}

	// 受控重建：若存在旧实例，先释放旧资源（失败则回滚新资源并保留可重试状态）。
	if ok {
		if err := e.drainOld(spec.Name, existing); err != nil {
			_ = e.rollback(spec.Name, b, "drain-old", err)
			return nil, err
		}
	}

	env, aerr := e.bringUp(ctx, spec, order, hash, resolved, bindingsBySvc, b, existing)
	if aerr != nil {
		isCreate := existing == nil
		// 标记失败并落盘（不含任何密钥值），随后回滚。
		failed := e.failedSnapshot(spec, hash, aerr, env, existing, b.quotaAcquired || (existing != nil && existing.QuotaHeld))
		_ = e.store.Put(failed)
		if rerr := e.rollback(spec.Name, b, "bring-up", aerr); rerr != nil {
			// 回滚自身失败：保留 Failed 快照与孤儿资源记录，错误可区分、可重试。
			// 创建路径名额在 rollback 中已可能被释放——若释放失败则保留在快照簿记中。
			return failed.Clone(), fmt.Errorf("apply failed (%v); %w", aerr, rerr)
		}
		if isCreate {
			// 全新创建失败且回滚成功：删除失败快照，环境视为未创建，可立即重新提交。
			_ = e.store.Delete(spec.Name)
			return nil, aerr
		}
		// 更新失败且回滚成功：保留旧/失败快照但释放其簿记资源，状态置为 Failed，
		// 名额沿用旧实例（未 drain 之外的新资源），等待用户重新 Apply 修复或 Delete。
		failed.Phase = domain.PhaseFailed
		failed.QuotaHeld = existing.QuotaHeld
		failed.UpdatedAt = time.Now()
		_ = e.store.Put(failed)
		return failed.Clone(), aerr
	}

	if perr := e.store.Put(env); perr != nil {
		if rerr := e.rollback(spec.Name, b, "persist", perr); rerr != nil {
			return nil, fmt.Errorf("persist failed (%v); %w", perr, rerr)
		}
		return nil, perr
	}
	b.updatedStore = true
	e.logf("apply: env=%s converged phase=%s generation=%d hash=%s", env.Name, env.Phase, env.Generation, hash)
	return env.Clone(), nil
}

// drainOld 在受控重建前停止旧实例的全部进程并归还旧端口。
// 每一步幂等：已停止的进程/已归还的端口不产生重复释放。
func (e *Engine) drainOld(envName string, old *domain.Environment) error {
	names := make([]string, 0, len(old.Services))
	for n := range old.Services {
		names = append(names, n)
	}
	sort.Strings(names)
	// 逆拓扑（近似为逆名字序的安全停法：这里按依赖关系严格逆序更稳）。
	// 旧实例记录中没有拓扑序，采用“谁被依赖谁后停”的策略：重复迭代直到稳定。
	stopped := map[string]bool{}
	for {
		progress := false
		for _, n := range names {
			if stopped[n] {
				continue
			}
			st := old.Services[n]
			allDepsStopped := true
			for _, d := range st.Spec.DependsOn {
				if old.Services[d] != nil && !stopped[d] {
					allDepsStopped = false
					break
				}
			}
			if !allDepsStopped {
				continue
			}
			if st.PID > 0 {
				if err := e.sandbox.Stop(envName, n, st.PID); err != nil {
					e.logf("drain: stop %s/%s pid=%d -> %v (treating as stopped if already gone)", envName, n, st.PID, err)
				} else {
					e.logf("drain: stopped env=%s svc=%s pid=%d", envName, n, st.PID)
				}
			}
			for _, pb := range st.Ports {
				if err := e.ports.Release(pb.Port); err != nil && !errorsIs(err, resources.ErrPortNotAllocated) {
					return err
				}
				e.logf("drain: released port=%d svc=%s", pb.Port, n)
			}
			stopped[n] = true
			progress = true
		}
		if !progress {
			break
		}
	}
	return nil
}

// bringUp 按拓扑顺序启动服务：上游未就绪不启动下游；上游失败则下游标记 Blocked。
func (e *Engine) bringUp(
	ctx context.Context,
	spec domain.EnvironmentSpec,
	order []string,
	hash string,
	resolved map[string]map[string]string,
	bindingsBySvc map[string][]domain.SecretBinding,
	b *resourceBundle,
	old *domain.Environment,
) (*domain.Environment, error) {
	svcByName := map[string]domain.ServiceSpec{}
	for _, svc := range spec.Services {
		svcByName[svc.Name] = svc
	}

	now := time.Now()
	generation := int64(1)
	if old != nil {
		generation = old.Generation + 1
	}
	env := &domain.Environment{
		Name:       spec.Name,
		Spec:       spec,
		SpecHash:   hash,
		Phase:      domain.PhaseCreating,
		Generation: generation,
		Services:   map[string]*domain.ServiceStatus{},
		CreatedAt:  now,
		UpdatedAt:  now,
		DataDir:    fmt.Sprintf("%s/sandbox/%s", e.cfg.StateDir, spec.Name),
		QuotaHeld:  b.quotaAcquired || (old != nil && old.QuotaHeld), // 更新继承旧名额
	}
	if old != nil {
		env.CreatedAt = old.CreatedAt
	}
	if spec.TTL > 0 {
		env.ExpiresAt = now.Add(spec.TTL)
	}

	blocked := map[string]bool{}
	for _, name := range order {
		select {
		case <-ctx.Done():
			return env, ctx.Err()
		default:
		}
		svc := svcByName[name]

		// 依赖裁决：任一上游未 Ready（Blocked/Failed/缺失）=> 下游 Blocked，绝不静默跳过。
		blockReason := ""
		for _, dep := range svc.DependsOn {
			up := env.Services[dep]
			if up == nil {
				blockReason = fmt.Sprintf("upstream %q absent", dep)
				break
			}
			if up.Phase != domain.ServiceReady {
				blockReason = fmt.Sprintf("upstream %q is %s (%s)", dep, up.Phase, up.Reason)
				break
			}
		}
		if blockReason != "" {
			blocked[name] = true
			env.Services[name] = &domain.ServiceStatus{
				Spec:    svc,
				Phase:   domain.ServiceBlocked,
				Secrets: bindingsBySvc[name],
				Reason:  blockReason,
			}
			e.logf("bringup: svc=%s BLOCKED env=%s reason=%s", name, spec.Name, blockReason)
			continue
		}

		// 端口分配：固定端口冲突或池满属于基础设施失败，整体回滚（QuotaExceededError）。
		// 临时记录在 pendingPorts 中：若随后的进程启动失败，这些端口立即释放并随
		// 服务失败进入 Degraded，而不是持有无用绑定。
		var bindings []domain.PortBinding
		var pendingPorts []int
		allocFailed := false
		for _, ps := range svc.Ports {
			port, err := e.ports.Allocate(ps.Fixed)
			if err != nil {
				// 先释放本服务本轮已分到的端口。
				for _, pp := range pendingPorts {
					_ = e.ports.Release(pp)
					b.allocated[pp] = false
				}
				env.Services[name] = &domain.ServiceStatus{
					Spec:    svc,
					Phase:   domain.ServiceFailed,
					Secrets: bindingsBySvc[name],
					Reason:  fmt.Sprintf("allocate port %s: %v", ps.Name, err),
				}
				blocked[name] = true
				allocFailed = true
				e.logf("bringup: svc=%s port-allocation FAILED env=%s err=%v -> degraded", name, spec.Name, err)
				break
			}
			b.allocated[port] = true
			pendingPorts = append(pendingPorts, port)
			bindings = append(bindings, domain.PortBinding{Name: ps.Name, Port: port})
			e.logf("bringup: port allocated env=%s svc=%s name=%s port=%d", spec.Name, name, ps.Name, port)
		}
		if allocFailed {
			// 端口不可用视为期望状态无法满足：不回滚其它已就绪服务，整体收敛为 Degraded。
			continue
		}
		sort.Slice(bindings, func(i, j int) bool { return bindings[i].Name < bindings[j].Name })

		pid, serr := e.sandbox.Start(sandboxStartReq(spec.Name, svc, resolved[name]))
		if serr != nil {
			// 服务自身启动失败：释放它刚分到的端口，标记 Failed，其下游在后续迭代
			// 经依赖裁决被标记为 Blocked——而不是被静默跳过，也不拖垮不相关分支。
			for _, pp := range pendingPorts {
				if rerr := e.ports.Release(pp); rerr == nil {
					b.allocated[pp] = false
					e.logf("bringup: released port=%d for failed svc=%s", pp, name)
				}
			}
			env.Services[name] = &domain.ServiceStatus{
				Spec:    svc,
				Phase:   domain.ServiceFailed,
				Secrets: bindingsBySvc[name],
				Reason:  serr.Error(),
			}
			blocked[name] = true
			e.logf("bringup: svc=%s FAILED env=%s err=%v -> degraded (downstream will be blocked)", name, spec.Name, serr)
			continue
		}
		b.pids[name] = pid
		b.started[name] = true
		e.logf("bringup: process started env=%s svc=%s pid=%d", spec.Name, name, pid)

		env.Services[name] = &domain.ServiceStatus{
			Spec:      svc,
			Phase:     domain.ServiceReady,
			Ports:     bindings,
			Secrets:   bindingsBySvc[name],
			PID:       pid,
			StartedAt: time.Now(),
		}
	}

	if len(blocked) > 0 {
		env.Phase = domain.PhaseDegraded
		env.Reason = fmt.Sprintf("%d service(s) blocked due to upstream failure", len(blocked))
	} else {
		env.Phase = domain.PhaseReady
	}
	env.UpdatedAt = time.Now()
	return env, nil
}

func (e *Engine) failedSnapshot(spec domain.EnvironmentSpec, hash string, cause error, partial *domain.Environment, old *domain.Environment, quotaHeld bool) *domain.Environment {
	if partial == nil {
		now := time.Now()
		partial = &domain.Environment{
			Name:      spec.Name,
			Spec:      spec,
			SpecHash:  hash,
			Phase:     domain.PhaseFailed,
			Services:  map[string]*domain.ServiceStatus{},
			CreatedAt: now,
			UpdatedAt: now,
		}
		if old != nil {
			partial.CreatedAt = old.CreatedAt
			partial.Generation = old.Generation + 1
		}
	} else {
		partial.Phase = domain.PhaseFailed
	}
	partial.QuotaHeld = quotaHeld
	partial.Reason = cause.Error()
	partial.UpdatedAt = time.Now()
	return partial
}

// errRetryCreate 表示更新路径持锁后发现环境已被删除，调用方应重走创建门控。
var errRetryCreate = errors.New("environment vanished during update; retry as create")

// createLocked 是创建领队的执行体：申请名额 -> 持环境锁 -> 二次确认不存在 ->
// 启动全部服务 -> 落盘。任何失败都按创建语义回滚（成功回滚后删除失败快照）。
func (e *Engine) createLocked(
	ctx context.Context,
	spec domain.EnvironmentSpec,
	order []string,
	hash string,
	resolved map[string]map[string]string,
	bindingsBySvc map[string][]domain.SecretBinding,
) (result *domain.Environment, retErr error) {
	if err := e.acquireQuota(ctx, spec.Name); err != nil {
		return nil, err
	}
	b := newResourceBundle()
	b.quotaAcquired = true
	e.logf("apply: quota acquired env=%s used=%d", spec.Name, e.quota.InUse())

	fl, unlock := e.lockEnvFully(spec.Name)
	defer unlock()
	_ = fl

	// 二次确认：领队等待期间理论上不会有他人持锁创建（门控本身已去重），
	// 但 Delete/Reconcile 恢复等路径可能改变状态，这里做防御性核对。
	if existing, ok, err := e.store.Get(spec.Name); err != nil {
		return nil, err
	} else if ok && existing.Phase != domain.PhaseDeleting &&
		existing.SpecHash == hash &&
		(existing.Phase == domain.PhaseReady || existing.Phase == domain.PhaseDegraded) {
		// 已存在且相同：让出名额并幂等返回。
		if rerr := e.releaseQuota(); rerr != nil {
			return nil, rerr
		}
		b.quotaAcquired = false
		return existing.Clone(), nil
	}

	env, aerr := e.bringUp(ctx, spec, order, hash, resolved, bindingsBySvc, b, nil)
	if aerr != nil {
		failed := e.failedSnapshot(spec, hash, aerr, env, nil, b.quotaAcquired)
		_ = e.store.Put(failed)
		if rerr := e.rollback(spec.Name, b, "bring-up", aerr); rerr != nil {
			return failed.Clone(), fmt.Errorf("apply failed (%v); %w", aerr, rerr)
		}
		// 创建失败且回滚成功：删除失败快照，环境视为未创建，可立即重新提交。
		_ = e.store.Delete(spec.Name)
		return nil, aerr
	}

	if perr := e.store.Put(env); perr != nil {
		if rerr := e.rollback(spec.Name, b, "persist", perr); rerr != nil {
			return nil, fmt.Errorf("persist failed (%v); %w", perr, rerr)
		}
		_ = e.store.Delete(spec.Name)
		return nil, perr
	}
	b.updatedStore = true
	e.logf("apply: env=%s converged phase=%s generation=%d hash=%s", env.Name, env.Phase, env.Generation, hash)
	return env.Clone(), nil
}
