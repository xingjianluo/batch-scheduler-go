package scheduler

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newPersistentScheduler(t *testing.T, path string, clk *fakeClock, cap_ Resource, tenants []Tenant) *Scheduler {
	t.Helper()
	s, err := New(Config{
		Capacity:     cap_,
		AgeIncrement: 10,
		AgingPeriod:  10 * time.Second,
		StorePath:    path,
		Clock:        clk,
	}, tenants)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// TestCrashRecovery 模拟进程异常退出后重启：
// 排队/运行占位/终态必须与崩溃前一致，终态作业不得再次被调度。
func TestCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	tenants := []Tenant{{ID: "a", Quota: Resource{CPU: 8}}}

	clk := newFakeClock()
	// 第一段生命周期：制造 运行/排队/完成/取消 四类作业。
	func() {
		s := newPersistentScheduler(t, path, clk, Resource{CPU: 4}, tenants)
		mustSubmit(t, s, "run-1", "a", 1, Resource{CPU: 2})
		mustSubmit(t, s, "run-2", "a", 1, Resource{CPU: 2})
		mustSubmit(t, s, "queue-1", "a", 1, Resource{CPU: 1}) // 集群满，排队
		mustSubmit(t, s, "done-1", "a", 5, Resource{CPU: 1})  // 抢占后完成
		// done-1 高优先级会抢占一个低优先级作业；直接把它完成。
		if _, err := s.Complete("done-1"); err != nil {
			t.Fatalf("complete done-1: %v", err)
		}
		mustSubmit(t, s, "queue-2", "a", 1, Resource{CPU: 1})
		if _, err := s.Cancel("queue-2"); err != nil {
			t.Fatalf("cancel queue-2: %v", err)
		}
		logSnapshot(t, "before-crash", s, nil)
		// 模拟崩溃：不经过任何优雅关闭，仅依赖每次变更后的原子持久化。
		s.mu.Lock()
		_ = s.persistLocked()
		s.mu.Unlock()
		_ = s.store.Close()
	}()

	// 第二段生命周期：从同一持久化文件重启。
	s2 := newPersistentScheduler(t, path, clk, Resource{CPU: 4}, tenants)
	t.Cleanup(func() { _ = s2.Close() })

	mustState(t, s2, "run-1", StateRunning)
	mustState(t, s2, "run-2", StateRunning)
	mustState(t, s2, "queue-1", StateQueued)
	mustState(t, s2, "done-1", StateCompleted)
	mustState(t, s2, "queue-2", StateCanceled)

	snap := s2.Snapshot()
	logSnapshot(t, "after-recovery", s2, nil)

	// 资源占位必须精确恢复（集群恰好占满，queue-1 仍不能启动）。
	assertResource(t, snap.Used, Resource{CPU: 4}, "recovered cluster usage")
	if len(snap.RunningIDs) != 2 {
		t.Fatalf("recovered running = %v, want 2", snap.RunningIDs)
	}
	if len(snap.QueuedIDs) != 1 || snap.QueuedIDs[0] != "queue-1" {
		t.Fatalf("recovered queue = %v, want [queue-1]", snap.QueuedIDs)
	}

	// 已完成/已取消的作业不得被再次调度或改变。
	if _, err := s2.Complete("done-1"); err != ErrJobAlreadyTerminal {
		t.Fatalf("complete terminal err = %v, want ErrJobAlreadyTerminal", err)
	}
	if _, err := s2.Cancel("queue-2"); err != ErrJobAlreadyTerminal {
		t.Fatalf("cancel terminal err = %v, want ErrJobAlreadyTerminal", err)
	}
	// 终态 ID 不可复用。
	if _, err := s2.Submit("done-1", "a", 1, Resource{CPU: 1}); !IsReject(err, RejectDuplicateJob) {
		t.Fatalf("reuse completed id err = %v", err)
	}

	// 恢复后释放资源，排队作业必须能继续推进（恢复不影响后续调度）。
	if _, err := s2.Complete("run-1"); err != nil {
		t.Fatalf("complete run-1 after recovery: %v", err)
	}
	mustState(t, s2, "queue-1", StateRunning)
	logSnapshot(t, "recovered-and-progressed", s2, nil)
}

// TestRecoverySubmitSeq 重启后提交序号必须延续，不会与旧作业序号冲突，
// 且新作业状态推进规则不变。
func TestRecoverySubmitSeq(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	tenants := []Tenant{{ID: "a", Quota: Resource{CPU: 100}}}

	clk := newFakeClock()
	func() {
		s := newPersistentScheduler(t, path, clk, Resource{CPU: 100}, tenants)
		mustSubmit(t, s, "first", "a", 0, Resource{CPU: 1})
		mustSubmit(t, s, "second", "a", 0, Resource{CPU: 1})
		_ = s.Close()
	}()
	s2 := newPersistentScheduler(t, path, clk, Resource{CPU: 100}, tenants)
	t.Cleanup(func() { _ = s2.Close() })
	mustSubmit(t, s2, "third", "a", 0, Resource{CPU: 1})
	j, err := s2.Get("third")
	if err != nil {
		t.Fatalf("get third: %v", err)
	}
	if j.SubmitSeq != 3 {
		t.Fatalf("third SubmitSeq = %d, want 3 (sequence must survive restart)", j.SubmitSeq)
	}
}

// TestRecoveryUnderConcurrentLoad 并发提交过程中“崩溃”后，
// 磁盘上的状态必须自洽：账目一致、无超卖、无重复占位。
func TestRecoveryUnderConcurrentLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	tenants := []Tenant{{ID: "a", Quota: Resource{CPU: 8}}}
	clk := newFakeClock()

	func() {
		s := newPersistentScheduler(t, path, clk, Resource{CPU: 8}, tenants)
		var wg sync.WaitGroup
		for i := 0; i < 40; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, _ = s.Submit("j-"+itoa(i), "a", i%4, Resource{CPU: 1})
			}(i)
		}
		wg.Wait()
		_ = s.store.Close() // 崩溃式退出
	}()

	s2 := newPersistentScheduler(t, path, clk, Resource{CPU: 8}, tenants)
	t.Cleanup(func() { _ = s2.Close() })
	snap := s2.Snapshot()
	if !snap.Capacity.Fits(snap.Used) {
		t.Fatalf("oversold after recovery: used=%+v", snap.Used)
	}
	var sum Resource
	ids := map[string]bool{}
	for _, j := range snap.Jobs {
		if ids[j.ID] {
			t.Fatalf("duplicated job after recovery: %s", j.ID)
		}
		ids[j.ID] = true
		if j.State == StateRunning {
			sum = sum.Add(j.Request)
		}
	}
	if sum != snap.Used {
		t.Fatalf("accounts inconsistent after recovery: used=%+v sum=%+v", snap.Used, sum)
	}
	if len(snap.Jobs) != 40 {
		t.Fatalf("recovered job count = %d, want 40", len(snap.Jobs))
	}
	t.Logf("recovered under load: running=%d queued=%d used=%+v",
		len(snap.RunningIDs), len(snap.QueuedIDs), snap.Used)
}
