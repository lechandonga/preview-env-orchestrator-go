// Package domain 定义预览环境编排器的核心声明式模型与运行时状态类型。
package domain

import "time"

// EnvironmentPhase 是环境实例的生命周期相位。
type EnvironmentPhase string

const (
	PhasePending    EnvironmentPhase = "Pending"
	PhaseCreating   EnvironmentPhase = "Creating"
	PhaseReady      EnvironmentPhase = "Ready"
	PhaseDegraded   EnvironmentPhase = "Degraded" // 部分服务因依赖阻塞/失败而不可用
	PhaseDeleting   EnvironmentPhase = "Deleting"
	PhaseFailed     EnvironmentPhase = "Failed"
	PhaseTerminated EnvironmentPhase = "Terminated"
)

// ServicePhase 是单个服务实例的相位。
type ServicePhase string

const (
	ServicePending  ServicePhase = "Pending"
	ServiceStarting ServicePhase = "Starting"
	ServiceReady    ServicePhase = "Ready"
	ServiceBlocked  ServicePhase = "Blocked" // 上游失败导致不被启动
	ServiceFailed   ServicePhase = "Failed"
	ServiceStopped  ServicePhase = "Stopped"
)

// SecretRef 声明对某个密钥的引用。Version=0 表示在绑定时解析为当前最新版本，
// 一旦环境创建完成即固化为具体版本；Version>0 表示必须绑定该版本。
type SecretRef struct {
	Name    string `json:"name"`
	Version int    `json:"version,omitempty"`
}

// PortSpec 声明服务端口；Fixed=0 表示由端口池动态分配。
type PortSpec struct {
	Name  string `json:"name"`
	Fixed int    `json:"fixed,omitempty"`
}

// ServiceSpec 是单个服务的声明式描述。
type ServiceSpec struct {
	Name      string            `json:"name"`
	Image     string            `json:"image"`
	DependsOn []string          `json:"dependsOn,omitempty"`
	Ports     []PortSpec        `json:"ports,omitempty"`
	Secrets   []SecretRef       `json:"secrets,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
}

// EnvironmentSpec 是一个预览环境的声明式描述。
type EnvironmentSpec struct {
	Name     string        `json:"name"`
	Services []ServiceSpec `json:"services"`
	TTL      time.Duration `json:"ttl,omitempty"` // >0 时为临时环境，到期自动回收
}

// PortBinding 记录一次实际的端口分配结果。
type PortBinding struct {
	Name string `json:"name"`
	Port int    `json:"port"`
}

// SecretBinding 记录密钥引用在环境创建/更新时解析到的固化版本。
type SecretBinding struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
}

// ServiceStatus 是服务的运行时状态。
type ServiceStatus struct {
	Spec      ServiceSpec     `json:"spec"`
	Phase     ServicePhase    `json:"phase"`
	Ports     []PortBinding   `json:"ports,omitempty"`
	Secrets   []SecretBinding `json:"secrets,omitempty"`
	PID       int             `json:"pid,omitempty"`
	Reason    string          `json:"reason,omitempty"`
	StartedAt time.Time       `json:"startedAt,omitempty"`
	StoppedAt time.Time       `json:"stoppedAt,omitempty"`
}

// Environment 是声明式描述收敛后得到的环境实例快照。
type Environment struct {
	Name       string                    `json:"name"`
	Spec       EnvironmentSpec           `json:"spec"`
	SpecHash   string                    `json:"specHash"`
	Phase      EnvironmentPhase          `json:"phase"`
	Generation int64                     `json:"generation"`
	Services   map[string]*ServiceStatus `json:"services"`
	Reason     string                    `json:"reason,omitempty"`
	CreatedAt  time.Time                 `json:"createdAt"`
	UpdatedAt  time.Time                 `json:"updatedAt"`
	ExpiresAt  time.Time                 `json:"expiresAt,omitempty"`
	// DataDir 是该环境占用的沙箱目录（用于 PID 文件/对账标记）。
	DataDir string `json:"dataDir,omitempty"`
	// QuotaHeld 表示该环境当前是否占有着一个环境名额，
	// 使失败残留快照的清理路径不会误归还（或漏归还）配额。
	QuotaHeld bool `json:"quotaHeld,omitempty"`
}

// Clone 返回环境快照的深拷贝，避免调用方持有的引用与内部状态发生数据竞争。
func (e *Environment) Clone() *Environment {
	if e == nil {
		return nil
	}
	cp := *e
	cp.Spec.Services = append([]ServiceSpec(nil), e.Spec.Services...)
	for i := range cp.Spec.Services {
		s := cp.Spec.Services[i]
		s.DependsOn = append([]string(nil), s.DependsOn...)
		s.Ports = append([]PortSpec(nil), s.Ports...)
		s.Secrets = append([]SecretRef(nil), s.Secrets...)
		if s.Env != nil {
			env := make(map[string]string, len(s.Env))
			for k, v := range s.Env {
				env[k] = v
			}
			s.Env = env
		}
		cp.Spec.Services[i] = s
	}
	if e.Services != nil {
		cp.Services = make(map[string]*ServiceStatus, len(e.Services))
		for k, v := range e.Services {
			sc := *v
			sc.Ports = append([]PortBinding(nil), v.Ports...)
			sc.Secrets = append([]SecretBinding(nil), v.Secrets...)
			sc.Spec.DependsOn = append([]string(nil), v.Spec.DependsOn...)
			sc.Spec.Ports = append([]PortSpec(nil), v.Spec.Ports...)
			sc.Spec.Secrets = append([]SecretRef(nil), v.Spec.Secrets...)
			if v.Spec.Env != nil {
				env := make(map[string]string, len(v.Spec.Env))
				for k2, v2 := range v.Spec.Env {
					env[k2] = v2
				}
				sc.Spec.Env = env
			}
			cp.Services[k] = &sc
		}
	}
	return &cp
}
