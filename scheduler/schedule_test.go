package scheduler

import (
	"testing"
)

func mustSubmit(t *testing.T, s *Scheduler, spec JobSpec) *Job {
	t.Helper()
	j, _, err := s.Submit(spec)
	if err != nil {
		t.Fatalf("submit %s: %v", spec.ID, err)
	}
	return j
}

// TestPriorityOrder 资源不足时高优先级先得到资源，低优先级排队。
func TestPriorityOrder(t *testing.T) {
	s := newSched(t, baseConfig(t.TempDir())) // 2000/2048
	mustSubmit(t, s, JobSpec{ID: "lo1", Tenant: "beta", Priority: 1, Request: Resources{CPUMillis: 800, MemoryMB: 800}, DurationTicks: 100})
	mustSubmit(t, s, JobSpec{ID: "lo2", Tenant: "beta", Priority: 1, Request: Resources{CPUMillis: 800, MemoryMB: 800}, DurationTicks: 100})
	mustSubmit(t, s, JobSpec{ID: "hi", Tenant: "beta", Priority: 5, Request: Resources{CPUMillis: 800, MemoryMB: 800}, DurationTicks: 2})
	// 同一 tick 内调度顺序必须为 hi 先运行（总容量只够两个）
	r := s.Tick()
	t.Logf("tick=%d started=%v preempted=%v 快照used=%s", r.Tick, r.Started, r.Preempted, s.Snapshot().Used)
	if s.Job("hi").State != StateRunning {
		t.Fatalf("hi 应运行, got %s", s.Job("hi").State)
	}
	running := map[string]bool{}
	for _, jb := range s.Snapshot().Jobs {
		running[jb.ID] = jb.State == StateRunning
	}
	// lo1/lo2 中恰好一个排队，且集群不得超卖
	if running["lo1"] == running["lo2"] {
		t.Fatalf("lo1/lo2 应恰有一个运行: lo1=%v lo2=%v", running["lo1"], running["lo2"])
	}
	if used := s.Snapshot().Used; !used.FitsIn(s.cfg.Capacity) {
		t.Fatalf("超卖: used=%s capacity=%s", used, s.cfg.Capacity)
	}
}

// TestPreemptAndResume 抢占后作业按确定规则重新入队，剩余时长继续推进直至完成。
func TestPreemptAndResume(t *testing.T) {
	s := newSched(t, baseConfig(t.TempDir()))
	// beta 独占型低优先级作业
	mustSubmit(t, s, JobSpec{ID: "low", Tenant: "beta", Priority: 1, Request: Resources{CPUMillis: 2000, MemoryMB: 2048}, DurationTicks: 5})
	s.Tick() // low 开始于 tick1，预计 tick6 完成
	// tick2 高优先级抢占（运行时长 2 个 tick: tick2 与 tick3，tick4 完成）
	mustSubmit(t, s, JobSpec{ID: "high", Tenant: "beta", Priority: 9, Request: Resources{CPUMillis: 2000, MemoryMB: 2048}, DurationTicks: 2})
	r2 := s.Tick()
	if len(r2.Preempted) != 1 || r2.Preempted[0] != "low" || s.Job("low").State != StatePending {
		t.Fatalf("tick2 应抢占 low: %+v low=%s", r2, s.Job("low").State)
	}
	if s.Job("low").EnqueueTick != 3 {
		t.Fatalf("被抢占者下一tick重新入队, enqueue=%d", s.Job("low").EnqueueTick)
	}
	if rem := s.Job("low").Remaining; rem != 4 {
		t.Fatalf("low 已运行1tick, 剩余应为4, got %d", rem)
	}
	if s.Job("low").Preempted != 1 {
		t.Fatalf("Preempted 计数=%d", s.Job("low").Preempted)
	}
	// tick3: high 继续运行, low 仍排队（容量被占）
	s.Tick()
	if s.Job("low").State != StatePending || s.Job("high").State != StateRunning {
		t.Fatalf("tick3: low=%s high=%s", s.Job("low").State, s.Job("high").State)
	}
	// tick4: high 完成, low 重新调度, 剩余4 tick
	r4 := s.Tick()
	t.Logf("tick4 completed=%v started=%v; low剩余=%d", r4.Completed, r4.Started, s.Job("low").Remaining)
	if s.Job("high").State != StateCompleted {
		t.Fatalf("high 应完成, got %s", s.Job("high").State)
	}
	if s.Job("low").State != StateRunning || s.Job("low").Remaining != 4 {
		t.Fatalf("low 应恢复运行且剩余4: state=%s rem=%d", s.Job("low").State, s.Job("low").Remaining)
	}
	// 再推进 4 tick（tick5..8）low 完成
	for i := 0; i < 4; i++ {
		s.Tick()
	}
	if s.Job("low").State != StateCompleted {
		t.Fatalf("low 应在恢复后4tick完成, got %s tick=%d", s.Job("low").State, s.CurrentTick())
	}
	// 终态作业不会被再次调度：再 tick 多次状态不变、used 为 0
	for i := 0; i < 3; i++ {
		s.Tick()
	}
	snap := s.Snapshot()
	if s.Job("low").State != StateCompleted || s.Job("high").State != StateCompleted || snap.Used.Any() {
		t.Fatalf("终态被再次调度? low=%s high=%s used=%s", s.Job("low").State, s.Job("high").State, snap.Used)
	}
}

// TestNoPreemptIfInfeasible 即使存在可抢占者，若抢完仍放不下则不得抢占（避免抖动）。
func TestNoPreemptIfInfeasible(t *testing.T) {
	s := newSched(t, baseConfig(t.TempDir()))
	// 运行 v1(600/600, prio1) 与 anchor(1400/1448, prio50 冻结高优先级)，恰好用满 2000/2048。
	// hi(prio9, 需求1500/1500) 只能抢占 v1，释放 600 后空闲仍不足 1500 → 不得抢占。
	mustSubmit(t, s, JobSpec{ID: "v1", Tenant: "beta", Priority: 1, Request: Resources{CPUMillis: 600, MemoryMB: 600}, DurationTicks: 100})
	mustSubmit(t, s, JobSpec{ID: "anchor", Tenant: "beta", Priority: 50, Request: Resources{CPUMillis: 1400, MemoryMB: 1448}, DurationTicks: 100})
	s.Tick()
	mustSubmit(t, s, JobSpec{ID: "hi", Tenant: "beta", Priority: 9, Request: Resources{CPUMillis: 1500, MemoryMB: 1500}, DurationTicks: 2})
	r := s.Tick()
	t.Logf("infeasible tick started=%v preempted=%v", r.Started, r.Preempted)
	if len(r.Preempted) != 0 {
		t.Fatalf("抢完仍不足时不应抢占, 实际 preempted=%v", r.Preempted)
	}
	if s.Job("v1").State != StateRunning || s.Job("hi").State != StatePending {
		t.Fatalf("v1=%s hi=%s", s.Job("v1").State, s.Job("hi").State)
	}
}

// TestAgingNoStarvation 低优先级作业在持续高优先级提交下，最终因老化获得调度。
func TestAgingNoStarvation(t *testing.T) {
	s := newSched(t, baseConfig(t.TempDir()))
	// 独占型低优先级"受害者"先不运行：先提交低优先级 long 占用，随后持续提交高优先级短作业。
	// 简化且确定的饿死检验：一个低优先级作业先排队，集群被一个"冻结高优先级"长作业占满，
	// 低优先级作业老化后有效优先级超过冻结值，应在其满足的 tick 触发抢占。
	mustSubmit(t, s, JobSpec{ID: "anchor", Tenant: "beta", Priority: 100, Request: Resources{CPUMillis: 2000, MemoryMB: 2048}, DurationTicks: 1000})
	s.Tick() // anchor 冻结 eff=100
	mustSubmit(t, s, JobSpec{ID: "old", Tenant: "beta", Priority: 0, Request: Resources{CPUMillis: 2000, MemoryMB: 2048}, DurationTicks: 1})
	// old 排队等待期间有效优先级随老化增长；无论通过抢占还是资源自然释放，
	// 它必须在有界时间内（anchor 最长运行到 tick1001）获得调度并完成。
	scheduledAt := int64(-1)
	for i := int64(0); i < 1030; i++ {
		r := s.Tick()
		if s.Job("old").State == StateRunning {
			scheduledAt = r.Tick
			t.Logf("old 在 tick=%d 获得调度（started=%v preempted=%v, 等待老化后 eff=%d）",
				r.Tick, r.Started, r.Preempted, s.Job("old").StartEffPrio)
			break
		}
	}
	if scheduledAt < 0 {
		t.Fatalf("低优先级作业被无限期饿死：1030 tick 内未获得调度")
	}
	// 获得调度后必须能推进到完成
	s.Tick()
	if s.Job("old").State != StateCompleted {
		t.Fatalf("old 应完成, got %s", s.Job("old").State)
	}
}

// TestDeterminism 相同初始状态与相同提交序列必须得到完全一致的调度结果。
func TestDeterminism(t *testing.T) {
	run := func() ([]Job, int64) {
		s := newSched(t, baseConfig(t.TempDir()))
		specs := []JobSpec{
			{ID: "a", Tenant: "alpha", Priority: 3, Request: Resources{CPUMillis: 700, MemoryMB: 700}, DurationTicks: 4},
			{ID: "b", Tenant: "beta", Priority: 2, Request: Resources{CPUMillis: 1500, MemoryMB: 1500}, DurationTicks: 6},
			{ID: "c", Tenant: "alpha", Priority: 5, Request: Resources{CPUMillis: 800, MemoryMB: 800}, DurationTicks: 2},
			{ID: "d", Tenant: "beta", Priority: 1, Request: Resources{CPUMillis: 500, MemoryMB: 500}, DurationTicks: 3},
			{ID: "e", Tenant: "alpha", Priority: 0, Request: Resources{CPUMillis: 300, MemoryMB: 300}, DurationTicks: 5},
		}
		for i, sp := range specs {
			mustSubmit(t, s, sp)
			s.Tick()
			_ = i
		}
		for k := 0; k < 20; k++ {
			s.Tick()
		}
		snap := s.Snapshot()
		return snap.Jobs, snap.Tick
	}
	jobs1, tick1 := run()
	jobs2, tick2 := run()
	if tick1 != tick2 || len(jobs1) != len(jobs2) {
		t.Fatalf("tick=%d/%d len=%d/%d", tick1, tick2, len(jobs1), len(jobs2))
	}
	for i := range jobs1 {
		a, b := jobs1[i], jobs2[i]
		if a != b {
			t.Fatalf("调度不确定: job[%d] 第一次=%+v 第二次=%+v", i, a, b)
		}
	}
	t.Logf("两次独立运行的 %d 个作业终态完全一致（tick=%d）", len(jobs1), tick1)
}
