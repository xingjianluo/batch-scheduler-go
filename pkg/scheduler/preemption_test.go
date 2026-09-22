package scheduler

import (
	"testing"
	"time"
)

// TestPriorityPreemption 高优先级作业抢占低优先级运行中作业；
// 被抢占作业重新入队，并在资源释放后按规则重新获得运行。
func TestPriorityPreemption(t *testing.T) {
	tenants := []Tenant{
		{ID: "a", Quota: Resource{CPU: 8}},
		{ID: "b", Quota: Resource{CPU: 8}},
	}
	s, clk := newTestScheduler(t, Resource{CPU: 4}, tenants)

	// 两个低优先级作业占满集群。
	mustSubmit(t, s, "low-1", "a", 1, Resource{CPU: 2})
	mustSubmit(t, s, "low-2", "b", 1, Resource{CPU: 2})
	logSnapshot(t, "low-jobs-running", s, nil)

	// 高优先级作业到达：应抢占一个低优先级作业。
	res, err := s.Submit("high-1", "a", 10, Resource{CPU: 2})
	if err != nil {
		t.Fatalf("high submit: %v", err)
	}
	if !res.Started {
		t.Fatal("high-1 should start via preemption")
	}
	snap := s.Snapshot()
	t.Logf("preempt check: running=%v queued=%v", snap.RunningIDs, snap.QueuedIDs)
	if !contains(snap.RunningIDs, "high-1") {
		t.Fatalf("high-1 not running: %v", snap.RunningIDs)
	}
	// low-1 与 high-1 同属租户 a：按“跨租户优先”的公平性平局规则，
	// 确定性的牺牲品必然是跨租户的 low-2（租户 b），而非同租户连环抢占。
	if contains(snap.RunningIDs, "low-2") {
		t.Fatalf("low-2 should have been preempted: %v", snap.RunningIDs)
	}
	if !contains(snap.RunningIDs, "low-1") {
		t.Fatalf("low-1 must keep running (same-tenant preemption is useless): %v", snap.RunningIDs)
	}
	mustState(t, s, "low-2", StateQueued)
	j, _ := s.Get("low-2")
	if j.PreemptedTimes != 1 {
		t.Fatalf("low-2 preempted times = %d, want 1", j.PreemptedTimes)
	}
	logSnapshot(t, "after-preemption", s, nil)

	// 释放高优先级作业后，被抢占作业应重新入队运行（状态继续推进）。
	clk.advance(time.Second)
	if _, err := s.Complete("high-1"); err != nil {
		t.Fatalf("complete high: %v", err)
	}
	mustState(t, s, "low-2", StateRunning)
	logSnapshot(t, "low-resumed", s, nil)
}

// TestEqualPriorityNoPreemption 相同（等效）优先级之间不得相互抢占。
func TestEqualPriorityNoPreemption(t *testing.T) {
	tenants := []Tenant{{ID: "a", Quota: Resource{CPU: 8}}}
	s, _ := newTestScheduler(t, Resource{CPU: 2}, tenants)
	mustSubmit(t, s, "first", "a", 5, Resource{CPU: 2})
	res, err := s.Submit("second", "a", 5, Resource{CPU: 2})
	if err != nil {
		t.Fatalf("second submit: %v", err)
	}
	if res.Started {
		t.Fatal("equal priority job must not preempt")
	}
	mustState(t, s, "first", StateRunning)
	mustState(t, s, "second", StateQueued)

	// 略高优先级（严格更大）才允许抢占。
	res2, err := s.Submit("third", "a", 6, Resource{CPU: 2})
	if err != nil {
		t.Fatalf("third submit: %v", err)
	}
	if !res2.Started {
		t.Fatal("strictly higher priority job should preempt")
	}
	logSnapshot(t, "equal-priority", s, nil)
}

// TestFIFOWithinPriority 同优先级按提交顺序 FIFO：
// 集群只够跑一个时，后到的小作业也不得插到先到的大作业之前。
func TestFIFOWithinPriority(t *testing.T) {
	tenants := []Tenant{{ID: "a", Quota: Resource{CPU: 10}}}
	s, _ := newTestScheduler(t, Resource{CPU: 4}, tenants)
	mustSubmit(t, s, "big", "a", 5, Resource{CPU: 3})   // 占 3
	mustSubmit(t, s, "large", "a", 5, Resource{CPU: 3}) // 排队（需 3，仅余 1）
	mustSubmit(t, s, "tiny", "a", 5, Resource{CPU: 1})  // 资源够，但按 FIFO 不得插队
	mustState(t, s, "large", StateQueued)
	mustState(t, s, "tiny", StateQueued)
	if q := s.Snapshot().QueuedIDs; len(q) != 2 || q[0] != "large" || q[1] != "tiny" {
		t.Fatalf("queue order = %v, want [large tiny]", q)
	}
	// big 完成后释放 3 个单位：调度按 FIFO 先启动 large（占 3），
	// 随后固定点内继续启动 tiny（占剩余 1）。
	// 关键结论：tiny 绝不能在 large 仍排队时先启动（无插队）。
	if _, err := s.Complete("big"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	mustState(t, s, "large", StateRunning)
	mustState(t, s, "tiny", StateRunning)
	logSnapshot(t, "fifo", s, nil)
}

// TestAgingNoStarvation 低优先级作业长时间等待后，因老化获得足够高的
// 等效优先级，可以抢占长期运行的高优先级作业，避免无限期饿死。
func TestAgingNoStarvation(t *testing.T) {
	tenants := []Tenant{{ID: "a", Quota: Resource{CPU: 10}}}
	s, clk := newTestScheduler(t, Resource{CPU: 2}, tenants)

	// 高优先级长跑作业（基础优先级 5），低优先级作业（0）排队。
	mustSubmit(t, s, "hp-runner", "a", 5, Resource{CPU: 2})
	mustSubmit(t, s, "lp-waiter", "a", 0, Resource{CPU: 2})
	mustState(t, s, "lp-waiter", StateQueued)

	// 等待 30s：老化 3*10=30，lp 等效优先级 30 > hp 的固定 5，
	// 推进时钟并触发 Tick 后应发生抢占。
	clk.advance(30 * time.Second)
	ds, err := s.Tick(clk.Now())
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	logSnapshot(t, "aging-tick", s, ds)
	mustState(t, s, "lp-waiter", StateRunning)
	mustState(t, s, "hp-runner", StateQueued)
}

// TestDeterminism 相同初始状态与相同提交序列，重复两次得到完全一致结果。
func TestDeterminism(t *testing.T) {
	tenants := []Tenant{
		{ID: "a", Quota: Resource{CPU: 8}},
		{ID: "b", Quota: Resource{CPU: 8}},
	}
	type op struct {
		id, tenant string
		pri        int
		req        Resource
		advance    time.Duration
	}
	ops := []op{
		{"j1", "a", 1, Resource{CPU: 2}, 0},
		{"j2", "b", 2, Resource{CPU: 3}, 0},
		{"j3", "a", 8, Resource{CPU: 2}, 0},
		{"j4", "b", 0, Resource{CPU: 1}, time.Second},
		{"j5", "a", 3, Resource{CPU: 4}, 2 * time.Second},
	}
	run := func() *Snapshot {
		clk := newFakeClock()
		s, err := New(Config{
			Capacity:     Resource{CPU: 4},
			AgeIncrement: 10,
			AgingPeriod:  10 * time.Second,
			Clock:        clk,
		}, tenants)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer s.Close()
		for _, o := range ops {
			clk.advance(o.advance)
			if _, err := s.Submit(o.id, o.tenant, o.pri, o.req); err != nil {
				t.Fatalf("submit %s: %v", o.id, err)
			}
		}
		clk.advance(40 * time.Second)
		if _, err := s.Tick(clk.Now()); err != nil {
			t.Fatalf("tick: %v", err)
		}
		return s.Snapshot()
	}
	s1 := run()
	s2 := run()
	if !snapshotsEqual(s1, s2) {
		t.Fatalf("non-deterministic:\n run1=%s\n run2=%s", dumpSnap(s1), dumpSnap(s2))
	}
	t.Logf("deterministic snapshot: %s", dumpSnap(s1))
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func snapshotsEqual(a, b *Snapshot) bool {
	if len(a.Jobs) != len(b.Jobs) {
		return false
	}
	for i := range a.Jobs {
		ja, jb := a.Jobs[i], b.Jobs[i]
		if ja.ID != jb.ID || ja.State != jb.State ||
			ja.PreemptedTimes != jb.PreemptedTimes || ja.SubmitSeq != jb.SubmitSeq {
			return false
		}
	}
	return equalStrings(a.QueuedIDs, b.QueuedIDs) && equalStrings(a.RunningIDs, b.RunningIDs)
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func dumpSnap(s *Snapshot) string {
	out := "queued=" + join(s.QueuedIDs) + " running=" + join(s.RunningIDs)
	for _, j := range s.Jobs {
		out += " | " + j.ID + ":" + j.State.String() + "(preempt=" +
			itoa(j.PreemptedTimes) + ",seq=" + itoa64(j.SubmitSeq) + ")"
	}
	return out
}

func join(xs []string) string {
	out := "["
	for i, x := range xs {
		if i > 0 {
			out += ","
		}
		out += x
	}
	return out + "]"
}

func itoa(n int) string { return itoa64(int64(n)) }
func itoa64(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
