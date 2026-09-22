package store

import (
	"os"
	"path/filepath"
	"syscall"
)

// FileLock 提供基于 flock(2) 的排他文件锁，用于跨进程/跨引擎实例串行化
// 同一环境上的调谐与销毁。锁在 Close（或进程退出由内核释放）后自动释放，
// 因此持锁进程崩溃不会留下永久死锁。
type FileLock struct {
	f *os.File
}

// LockDir 在 dir 下以 name 对应的锁文件获取排他锁（阻塞直到获得）。
func LockDir(dir, name string) (*FileLock, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "locks", safeName(name)+".lock")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &FileLock{f: f}, nil
}

// Unlock 释放排他锁并关闭文件。重复调用是安全的。
func (l *FileLock) Unlock() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	err := l.f.Close()
	l.f = nil
	return err
}
