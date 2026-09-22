package engine

import (
	"fmt"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"
)

// recoverBookkeeping 在引擎启动时依据持久化快照重建内存簿记：
//   - 端口池：把快照中每个 Ready/其它存活服务绑定的端口重新登记；
//   - 配额：计数 QuotaHeld=true 的环境；
//   - 进程：PID 尚存活的保持原 PID（PID 由操作系统保证唯一），已死亡的不在此处重启，
//     交由显式 Reconcile/ReconcileAll 决定（启动恢复默认不自动变更状态，只重建簿记），
//     并将死亡进程对应服务标记为 Pending，等待对账修复，避免簿记指向僵尸 PID。
//
// 发现端口/配额簿记冲突（例如快照间重复端口）时返回错误，拒绝带着损坏簿记启动。
func (e *Engine) recoverBookkeeping() error {
	// 绑定跨进程文件配额与文件端口池（多个引擎实例共享同一状态目录时的权威簿记）。
	if err := e.quota.BindStateDir(e.cfg.StateDir); err != nil {
		return err
	}
	if err := e.ports.BindStateDir(e.cfg.StateDir); err != nil {
		return err
	}
	// 先把文件计数归零，随后以快照为权威重建，避免与历史计数叠加。
	for e.quota.InUse() > 0 {
		if err := e.quota.Release(); err != nil {
			break
		}
	}

	envs, err := e.store.List()
	if err != nil {
		return fmt.Errorf("recover: list snapshots: %w", err)
	}

	held := 0
	knownPorts := map[int]string{} // port -> env/svc，检测跨环境重复绑定
	for _, env := range envs {
		if env.Phase == domain.PhaseTerminated {
			continue
		}
		if env.QuotaHeld && env.Phase != domain.PhaseDeleting {
			held++
		}
		if env.Phase == domain.PhaseDeleting && env.QuotaHeld {
			// 上次删除中断：名额仍然占着，先记账，后续 Reconcile 会完成删除并释放。
			held++
		}
		for svc, st := range env.Services {
			for _, pb := range st.Ports {
				if owner, dup := knownPorts[pb.Port]; dup {
					return fmt.Errorf("recover: port %d bound twice (%s and %s/%s)", pb.Port, owner, env.Name, svc)
				}
				knownPorts[pb.Port] = env.Name + "/" + svc
			}
		}
	}

	// 以快照端口集合为权威重建端口位图（内部校验区间与跨环境唯一性）。
	portsList := make([]int, 0, len(knownPorts))
	for port := range knownPorts {
		portsList = append(portsList, port)
	}
	if err := e.ports.ReconcileFromSnapshot(portsList); err != nil {
		return fmt.Errorf("recover: rebuild port bitmap: %w", err)
	}
	// 重建配额计数：通过 acquire/ release 对齐到 held。
	for i := 0; i < held; i++ {
		if !e.quota.TryAcquire() {
			return fmt.Errorf("recover: quota exceeded while restoring %d held environments (max=%d)", held, e.cfg.MaxEnvironments)
		}
	}

	// 进程存活探测：把已死亡进程的服务置为 Pending（等待 Reconcile 重启）。
	mutated := 0
	for _, env := range envs {
		changed := false
		for _, st := range env.Services {
			if st.PID > 0 && st.Phase == domain.ServiceReady && !e.sandbox.IsReady(st.PID) {
				e.logf("recover: env=%s svc=%s pid=%d dead -> Pending (awaiting reconcile)", env.Name, st.Spec.Name, st.PID)
				st.Phase = domain.ServicePending
				st.Reason = "process missing at startup recovery"
				st.PID = 0
				changed = true
			}
		}
		if changed {
			env.Phase = domain.PhaseDegraded
			env.Reason = "recovery detected missing processes"
			env.UpdatedAt = e.clockNow()
			if err := e.store.Put(env); err != nil {
				return fmt.Errorf("recover: persist drift for %s: %w", env.Name, err)
			}
			mutated++
		}
	}
	e.logf("recover: restored bookkeeping envs=%d heldQuota=%d ports=%d drifted=%d",
		len(envs), held, len(knownPorts), mutated)
	return nil
}
