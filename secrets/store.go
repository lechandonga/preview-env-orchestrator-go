// Package secrets 提供带版本的密钥存储，密钥只能被引用。
package secrets

import "errors"

var (
	ErrNotFound        = errors.New("secret not found")
	ErrVersionNotFound = errors.New("secret version not found")
)

// Store 是线程安全的版本化密钥存储。
type Store struct {
	versions map[string][]string // key -> 版本列表，索引+1 即版本号
}

func NewStore() *Store { return &Store{versions: map[string][]string{}} }

// Put 创建密钥首个版本，返回版本号 1。
func (s *Store) Put(key, value string) (int, error) { return 0, nil }

// Rotate 轮换密钥，追加新版本并返回版本号。
func (s *Store) Rotate(key, value string) (int, error) { return 0, nil }

// Get 读取指定版本；version=0 表示最新版本。缺失必须报错，不得退化为默认值。
func (s *Store) Get(key string, version int) (string, error) { return "", nil }

// Latest 返回 key 的最新版本号。
func (s *Store) Latest(key string) (int, error) { return 0, nil }
