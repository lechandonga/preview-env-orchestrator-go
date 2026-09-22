package resources

import (
	"os"
	"path/filepath"
	"strconv"
	"syscall"
)

// FSQuota 是跨进程共享目录的环境数配额：用一个带 flock 保护的计数文件
// 实现分布式信号量，保证多个引擎实例指向同一状态目录时限额仍然成立。
//
// 计数文件中存当前占用数；所有“读-判断-写”都在排他锁内完成。
type FSQuota struct {
	path string
	lock string
}

// NewFSQuota 在 dir 下创建/打开跨进程配额。
func NewFSQuota(dir string) (*FSQuota, error) {
	if err := os.MkdirAll(filepath.Join(dir, "quota"), 0o755); err != nil {
		return nil, err
	}
	q := &FSQuota{
		path: filepath.Join(dir, "quota", "count"),
		lock: filepath.Join(dir, "quota", "count.lock"),
	}
	if _, err := os.Stat(q.path); os.IsNotExist(err) {
		if err := os.WriteFile(q.path, []byte("0"), 0o600); err != nil {
			return nil, err
		}
	}
	return q, nil
}

func (q *FSQuota) withLock(fn func(f *os.File) error) error {
	lf, err := os.OpenFile(q.lock, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lf.Close()
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lf.Fd()), syscall.LOCK_UN)
	f, err := os.OpenFile(q.path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	return fn(f)
}

func readCount(f *os.File) int {
	buf := make([]byte, 32)
	n, _ := f.ReadAt(buf, 0)
	v, _ := strconv.Atoi(string(buf[:n]))
	if v < 0 {
		v = 0
	}
	return v
}

func writeCount(f *os.File, v int) error {
	if v < 0 {
		v = 0
	}
	if err := f.Truncate(0); err != nil {
		return err
	}
	_, err := f.WriteAt([]byte(strconv.Itoa(v)), 0)
	return err
}

// TryAcquire 尝试占用一个名额；满员返回 false。
func (q *FSQuota) TryAcquire(max int) (bool, error) {
	ok := false
	err := q.withLock(func(f *os.File) error {
		cur := readCount(f)
		if max > 0 && cur >= max {
			return nil
		}
		if err := writeCount(f, cur+1); err != nil {
			return err
		}
		ok = true
		return nil
	})
	return ok, err
}

// Release 归还一个名额；超量归还返回错误。
func (q *FSQuota) Release() error {
	return q.withLock(func(f *os.File) error {
		cur := readCount(f)
		if cur == 0 {
			return ErrQuotaOverdraw
		}
		return writeCount(f, cur-1)
	})
}

// InUse 返回当前占用数。
func (q *FSQuota) InUse() int {
	v := 0
	_ = q.withLock(func(f *os.File) error { v = readCount(f); return nil })
	return v
}
