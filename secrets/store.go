// Package secrets 提供带版本的密钥存储，密钥只能被引用。
package secrets

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrNotFound        = errors.New("secret not found")
	ErrVersionNotFound = errors.New("secret version not found")
	ErrExists          = errors.New("secret already exists")
	ErrRevoked         = errors.New("secret has been revoked")
)

// Store 是线程安全的版本化密钥存储。
type Store struct {
	mu       sync.RWMutex
	versions map[string][]string         // key -> 版本列表，索引+1 即版本号
	revoked  map[string]map[int]struct{} // key -> 已吊销版本集合
}

func NewStore() *Store {
	return &Store{versions: map[string][]string{}, revoked: map[string]map[int]struct{}{}}
}

// Put 创建密钥首个版本，返回版本号 1。
func (s *Store) Put(key, value string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.versions[key]; ok {
		return 0, ErrExists
	}
	s.versions[key] = []string{value}
	return 1, nil
}

// Rotate 轮换密钥，追加新版本并返回版本号。
func (s *Store) Rotate(key, value string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	vs, ok := s.versions[key]
	if !ok {
		return 0, ErrNotFound
	}
	s.versions[key] = append(vs, value)
	return len(s.versions[key]), nil
}

// Revoke 吊销指定版本；version=0 吊销该密钥的全部版本（含历史版本）。
// 吊销后任何对该版本的 Get 都返回 ErrRevoked，绝不退化为最新版本。
func (s *Store) Revoke(key string, version int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	vs, ok := s.versions[key]
	if !ok {
		return ErrNotFound
	}
	if version == 0 {
		set := map[int]struct{}{}
		for i := range vs {
			set[i+1] = struct{}{}
		}
		s.revoked[key] = set
		return nil
	}
	if version < 0 || version > len(vs) {
		return ErrVersionNotFound
	}
	set := s.revoked[key]
	if set == nil {
		set = map[int]struct{}{}
		s.revoked[key] = set
	}
	set[version] = struct{}{}
	return nil
}

// Get 读取指定版本；version=0 表示最新版本。缺失必须报错，不得退化为默认值。
func (s *Store) Get(key string, version int) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	vs, ok := s.versions[key]
	if !ok {
		return "", ErrNotFound
	}
	if version == 0 {
		version = len(vs)
	}
	if version < 0 || version > len(vs) {
		return "", ErrVersionNotFound
	}
	if set, ok := s.revoked[key]; ok {
		if _, bad := set[version]; bad {
			return "", fmt.Errorf("%w: %s@%d", ErrRevoked, key, version)
		}
	}
	return vs[version-1], nil
}

// Latest 返回 key 的最新版本号。
func (s *Store) Latest(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	vs, ok := s.versions[key]
	if !ok {
		return 0, ErrNotFound
	}
	return len(vs), nil
}
