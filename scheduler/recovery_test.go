package scheduler

import (
	"os"
	"testing"
)

// crashReopen 模拟进程崩溃后用同一 DataDir 重启（不注册租户，完全依赖 WAL 恢复）。
func crashReopen(t *testing.T, dir string) *Scheduler {
	t.Helper()
	s, err := New(Config{
		Capacity: Resources{CPUMillis: 2000, MemoryMB: 2048},
		DataDir:  dir,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestCrashRecoveryPendingAndRunning 崩溃重启后：排队/占位/终态全部保持一致。
func TestCrashRecoveryPendingAndRunning(t *testing.T) {
	dir := t.TempDir()
	s := newSched(t, baseConfig(dir))

	mustSubmit(t, s, JobSpec{ID: "run1", Tenant: "alpha", Priority: 1, Request: Resources{CPUMillis: 600, MemoryMB: 600}, DurationTicks: 5})
	s.Tick() // run1 运行中
	mustSubmit(t, s, JobSpec{ID: "wait1", Tenant: "alpha", Priority: 1, Request: Resources{CPUMillis: 600, MemoryMB: 600}, DurationTicks: 2})
	mustSubmit(t, s, JobSpec{ID: "done1", Tenant: "beta", Priority: 1, Request: Resources{CPUMillis: 200, MemoryMB: 200}, DurationTicks: 1})
	s.Tick() // done1 完成
	mustSubmit(t, s, JobSpec{ID: "cancel1", Tenant: "beta", Priority: 1, Request: Resources{CPUMillis: 200, MemoryMB: 200}, DurationTicks: 4})
	s.Tick()
	if err := s.Cancel("beta", "cancel1"); err != nil {
		t.Fatal(err)
	}
	before := s.Snapshot()
	t.Logf("崩溃前快照: tick=%d used=%s run1=%s wait1=%s done1=%s cancel1=%s",
		before.Tick, before.Used, s.Job("run1").State, s.Job("wait1").State, s.Job("done1").State, s.Job("cancel1").State)

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := crashReopen(t, dir)
	after := s2.Snapshot()
	t.Logf("恢复后快照: tick=%d used=%s", after.Tick, after.Used)

	if after.Tick != before.Tick {
		t.Fatalf("tick 不一致: before=%d after=%d", before.Tick, after.Tick)
	}
	if after.Used != before.Used {
		t.Fatalf("占用不一致: before=%s after=%s", before.Used, after.Used)
	}
	want := map[string]JobState{
		"run1": StateRunning, "wait1": StatePending,
		"done1": StateCompleted, "cancel1": StateCanceled,
	}
	for id, st := range want {
		if got := s2.Job(id).State; got != st {
			t.Fatalf("job %s: before=%s after=%s", id, st, got)
		}
	}
	// run1 在 tick1 派发（剩余5），崩溃于 tick3 处理之后，已运行 tick1/2/3 三个时间片，剩余 2
	if rem := s2.Job("run1").Remaining; rem != 2 {
		t.Fatalf("run1 remaining=%d want 2", rem)
	}
}

// TestCrashRecoveryContinues 恢复后继续推进，调度结论与"未崩溃"的并行参照一致。
func TestCrashRecoveryContinues(t *testing.T) {
	specs := []JobSpec{
		{ID: "a", Tenant: "alpha", Priority: 3, Request: Resources{CPUMillis: 700, MemoryMB: 700}, DurationTicks: 4},
		{ID: "b", Tenant: "beta", Priority: 2, Request: Resources{CPUMillis: 1500, MemoryMB: 1500}, DurationTicks: 6},
		{ID: "c", Tenant: "alpha", Priority: 5, Request: Resources{CPUMillis: 800, MemoryMB: 800}, DurationTicks: 2},
	}
	// 参照：无崩溃连续执行
	reference := func() []Job {
		s := newSched(t, baseConfig(t.TempDir()))
		for _, sp := range specs {
			mustSubmit(t, s, sp)
			s.Tick()
		}
		for k := 0; k < 25; k++ {
			s.Tick()
		}
		return s.Snapshot().Jobs
	}
	ref := reference()

	// 崩溃序列：与参照在完全相同的 tick 时刻提交相同作业，
	// 但在若干 tick 边界插入崩溃重开；额外 tick 会在结尾对齐扣除。
	dir2 := t.TempDir()
	s := newSched(t, baseConfig(dir2))
	crashBefore := map[int]bool{1: true, 2: true, 3: true} // 第1/2/3次提交前先崩溃重开
	ticksSoFar := int64(0)
	for idx, sp := range specs {
		if crashBefore[idx] {
			s.Close()
			s = crashReopen(t, dir2)
		}
		mustSubmit(t, s, sp)
		s.Tick()
		ticksSoFar++
	}
	// 参照在此刻已走 3 tick，崩溃序列同样 3 tick（崩溃本身不推进 tick）。
	// 剩余推进次数与参照完全一致。
	for k := int64(0); k < 25-ticksSoFar; k++ {
		s.Tick()
	}
	got := s.Snapshot().Jobs
	if len(got) != len(ref) {
		t.Fatalf("作业数 %d != %d", len(got), len(ref))
	}
	for i := range ref {
		if got[i] != ref[i] {
			t.Fatalf("崩溃恢复后调度结论不一致:\n ref=%+v\n got=%+v", ref[i], got[i])
		}
	}
	t.Logf("多次崩溃恢复后的终态与连续执行完全一致（%d 个作业）", len(ref))
}

// TestTerminalJobsNotRescheduled 已完成/已取消作业在重启后绝不被再次调度。
func TestTerminalJobsNotRescheduled(t *testing.T) {
	dir := t.TempDir()
	s := newSched(t, baseConfig(dir))
	mustSubmit(t, s, JobSpec{ID: "fin", Tenant: "alpha", Priority: 1, Request: Resources{CPUMillis: 300, MemoryMB: 300}, DurationTicks: 1})
	mustSubmit(t, s, JobSpec{ID: "kill", Tenant: "alpha", Priority: 1, Request: Resources{CPUMillis: 300, MemoryMB: 300}, DurationTicks: 5})
	s.Tick() // 两者都运行
	s.Tick() // fin 完成
	if err := s.Cancel("alpha", "kill"); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2 := crashReopen(t, dir)
	for k := 0; k < 10; k++ {
		s2.Tick()
	}
	if s2.Job("fin").State != StateCompleted || s2.Job("kill").State != StateCanceled {
		t.Fatalf("fin=%s kill=%s", s2.Job("fin").State, s2.Job("kill").State)
	}
	if used := s2.Snapshot().Used; used.Any() {
		t.Fatalf("终态作业仍占用资源: %s", used)
	}
	if j := s2.Job("fin"); j.Runs != 1 {
		t.Fatalf("fin Runs=%d, 终态作业被重复调度", j.Runs)
	}
	t.Logf("终态稳定: fin(COMPLETED,runs=1) kill(CANCELED) used=%s", s2.Snapshot().Used)
}

// TestWALFileDurable WAL 文件真实落盘（进程退出后文件存在且非空）。
func TestWALFileDurable(t *testing.T) {
	dir := t.TempDir()
	s := newSched(t, baseConfig(dir))
	mustSubmit(t, s, JobSpec{ID: "p", Tenant: "alpha", Priority: 1, Request: Resources{CPUMillis: 100, MemoryMB: 100}, DurationTicks: 1})
	s.Tick()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(dir + "/wal.jsonl")
	if err != nil {
		t.Fatalf("wal 文件不存在: %v", err)
	}
	if fi.Size() == 0 {
		t.Fatal("wal 文件为空")
	}
	t.Logf("WAL 已持久化: %s (%d bytes)", dir+"/wal.jsonl", fi.Size())
}
