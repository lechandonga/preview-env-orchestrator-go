// Package secret 提供仅按引用访问、带版本与轮换语义的密钥存储。
//
// 不变量：
//   - 密钥只能按 (名称, 版本) 引用解析，引擎不得把解析出的值写入状态文件或日志。
//   - 引用缺失或版本失效必须返回错误，绝不退化为空值/默认值。
//   - Rotate 之后，version=0 的引用解析到新版本；已显式绑定旧版本的引用
//     继续读到旧版本，除非该版本被 DisableVersion 吊销。
package secret

import (
	"errors"
	"fmt"
	"sync"
)

// 哨兵错误，便于调用方区分失败原因。
var (
	ErrNotFound        = errors.New("secret not found")
	ErrVersionNotFound = errors.New("secret version not found")
	ErrVersionDisabled = errors.New("secret version disabled")
)

type versionEntry struct {
	value    string
	disabled bool
}

type entry struct {
	versions []versionEntry // 下标 0 对应版本 1
	current  int            // 当前启用版本号（>=1）
}

// Store 保存具名密钥的多个版本。
type Store struct {
	mu      sync.RWMutex
	secrets map[string]*entry
}

// NewStore 创建空密钥存储。
func NewStore() *Store {
	return &Store{secrets: map[string]*entry{}}
}

// Put 新建一个密钥并返回其初始版本号（从 1 开始）。
// 重名创建是明确的错误（调用方应改用 Rotate）。
func (s *Store) Put(name, value string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.secrets[name]; ok {
		return 0, fmt.Errorf("secret %q already exists; use Rotate for a new version", name)
	}
	s.secrets[name] = &entry{
		versions: []versionEntry{{value: value}},
		current:  1,
	}
	return 1, nil
}

// Rotate 为密钥写入新版本并返回新版本号。密钥必须已存在。
func (s *Store) Rotate(name, newValue string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.secrets[name]
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrNotFound, name)
	}
	e.versions = append(e.versions, versionEntry{value: newValue})
	e.current = len(e.versions)
	return e.current, nil
}

// Current 返回密钥当前启用版本；缺失时返回包装了 ErrNotFound 的错误。
func (s *Store) Current(name string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.secrets[name]
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrNotFound, name)
	}
	return e.current, nil
}

// Resolve 解析指定密钥的指定版本值；version 为 0 表示当前版本。
// 密钥缺失、版本不存在或版本已停用都必须返回错误，绝不返回默认值。
func (s *Store) Resolve(name string, version int) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.secrets[name]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrNotFound, name)
	}
	if version == 0 {
		version = e.current
	}
	if version < 1 || version > len(e.versions) {
		return "", fmt.Errorf("%w: %q v%d", ErrVersionNotFound, name, version)
	}
	v := e.versions[version-1]
	if v.disabled {
		return "", fmt.Errorf("%w: %q v%d", ErrVersionDisabled, name, version)
	}
	return v.value, nil
}

// DisableVersion 停用某个版本（模拟版本失效/吊销）。
// 已绑定该版本的既有环境在下次重新解析（对账/重建该服务）时会得到明确失败。
func (s *Store) DisableVersion(name string, version int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.secrets[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrNotFound, name)
	}
	if version < 1 || version > len(e.versions) {
		return fmt.Errorf("%w: %q v%d", ErrVersionNotFound, name, version)
	}
	e.versions[version-1].disabled = true
	return nil
}
