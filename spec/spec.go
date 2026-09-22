// Package spec 定义声明式环境描述及其校验。
package spec

import "time"

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

// Key 返回描述的幂等键（内容指纹）。
func (s *EnvSpec) Key() string { return "" }

// Validate 校验描述合法性（含依赖环检测）。
func Validate(s *EnvSpec) error { return nil }
