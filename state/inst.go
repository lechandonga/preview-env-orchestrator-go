package state

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Instance 是一个引擎实例的心跳记录。
type Instance struct {
	ID         string    `json:"id"`
	PID        int       `json:"pid"`
	StartTicks int64     `json:"start_ticks"`
	StartedAt  time.Time `json:"started_at"`
	BeatAt     time.Time `json:"beat_at"`
}

func instRel(id string) string { return filepath.Join("inst", id+".json") }

// NewInstanceID 生成随机实例 ID（同一台机器上可忽略碰撞概率）。
func NewInstanceID() string {
	var b [8]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// RegisterInstance 写入本实例心跳记录；PID 与启动时间戳用于跨实例判活。
func (s *Store) RegisterInstance(id string) (*Instance, error) {
	pid := os.Getpid()
	ticks, err := readStartTicks(pid)
	if err != nil {
		return nil, err
	}
	inst := &Instance{
		ID:         id,
		PID:        pid,
		StartTicks: ticks,
		StartedAt:  time.Now(),
		BeatAt:     time.Now(),
	}
	if err := s.WriteJSON(instRel(id), inst); err != nil {
		return nil, err
	}
	return inst, nil
}

// Heartbeat 刷新本实例心跳（调用方自行决定是否持锁；原子写本身安全）。
func (s *Store) Heartbeat(inst *Instance) error {
	inst.BeatAt = time.Now()
	return s.WriteJSON(instRel(inst.ID), inst)
}

// Unregister 删除本实例心跳（正常退出用；强杀场景由接管方按超时判定）。
func (s *Store) Unregister(inst *Instance) error {
	return s.Remove(instRel(inst.ID))
}

// ListInstances 读回全部实例心跳记录。
func (s *Store) ListInstances() ([]*Instance, error) {
	names, err := s.List("inst")
	if err != nil {
		return nil, err
	}
	var out []*Instance
	for _, n := range names {
		var in Instance
		ok, err := s.ReadJSON(filepath.Join("inst", n), &in)
		if err != nil || !ok {
			continue // 并发删除/半写：跳过，下一轮再看
		}
		out = append(out, &in)
	}
	return out, nil
}

// IsAlive 判定实例是否仍存活：
// 心跳未超时，或心跳超时但进程身份（PID + 启动时间戳）仍匹配。
// 这样短时间心跳抖动不会误接管，而 PID 复用也不会误判为存活。
func (s *Store) IsAlive(in *Instance, now time.Time, timeout time.Duration) bool {
	if now.Sub(in.BeatAt) <= timeout {
		return true
	}
	ticks, err := readStartTicks(in.PID)
	if err != nil {
		return false
	}
	return ticks == in.StartTicks
}

// ReapStale 删除已死亡实例的心跳记录，返回被清理的实例（调用方持全局锁）。
func (s *Store) ReapStale(now time.Time, timeout time.Duration, selfID string) ([]*Instance, error) {
	all, err := s.ListInstances()
	if err != nil {
		return nil, err
	}
	var stale []*Instance
	for _, in := range all {
		if in.ID == selfID {
			continue
		}
		if !s.IsAlive(in, now, timeout) {
			if err := s.Remove(instRel(in.ID)); err != nil {
				return stale, err
			}
			stale = append(stale, in)
		}
	}
	return stale, nil
}

// readStartTicks 读取本进程 /proc/<pid>/stat 的启动时间戳。
func readStartTicks(pid int) (int64, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	s := string(data)
	rp := -1
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ')' {
			rp = i
			break
		}
	}
	if rp < 0 {
		return 0, errors.New("malformed stat")
	}
	fields := strings.Fields(s[rp+1:])
	const startIdx = 22 - 3 // ')' 后切片：starttime(总第22列)
	if len(fields) <= startIdx {
		return 0, errors.New("short stat")
	}
	return strconv.ParseInt(fields[startIdx], 10, 64)
}
