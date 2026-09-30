package state

import (
	"path/filepath"
	"testing"
	"time"
)

func TestAtomicJSONAndLock(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.WriteJSON("envs/a.json", map[string]int{"port": 31000}); err != nil {
		t.Fatal(err)
	}
	var v map[string]int
	ok, err := s.ReadJSON("envs/a.json", &v)
	if err != nil || !ok || v["port"] != 31000 {
		t.Fatalf("读回失败: ok=%v err=%v v=%v", ok, err, v)
	}
	if ok, _ := s.ReadJSON("envs/missing.json", &v); ok {
		t.Fatal("缺失文件应返回 ok=false")
	}
	unlock := s.Lock()
	ch := make(chan struct{})
	go func() {
		s2, err := Open(s.Dir())
		if err != nil {
			t.Error(err)
			close(ch)
			return
		}
		u := s2.Lock()
		close(ch)
		u()
	}()
	select {
	case <-ch:
		t.Fatal("排他锁未释放时第二个加锁不应返回")
	case <-time.After(50 * time.Millisecond):
	}
	unlock()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("释放后第二个加锁应返回")
	}
	if err := s.AppendJournal(map[string]string{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	lines, err := s.ReadJournal()
	if err != nil || len(lines) != 1 {
		t.Fatalf("journal lines=%d err=%v", len(lines), err)
	}
}

func TestInstanceHeartbeatAndReap(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	in, err := s.RegisterInstance(NewInstanceID())
	if err != nil {
		t.Fatal(err)
	}
	if !s.IsAlive(in, time.Now(), time.Second) {
		t.Fatal("刚注册的实例必须存活")
	}
	// 伪造一个心跳超时、PID 不可能存在的实例：应被判死并清理。
	dead := &Instance{ID: NewInstanceID(), PID: 1 << 22, StartTicks: 1, BeatAt: time.Now().Add(-time.Hour)}
	if err := s.WriteJSON(filepath.Join("inst", dead.ID+".json"), dead); err != nil {
		t.Fatal(err)
	}
	if s.IsAlive(dead, time.Now(), time.Second) {
		t.Fatal("PID 不存在且心跳超时的实例必须判死")
	}
	stale, err := s.ReapStale(time.Now(), time.Second, in.ID)
	if err != nil || len(stale) != 1 {
		t.Fatalf("应收割 1 个死实例, got %d err=%v", len(stale), err)
	}
	all, _ := s.ListInstances()
	if len(all) != 1 {
		t.Fatalf("收割后应只剩自身, got %d", len(all))
	}
}
