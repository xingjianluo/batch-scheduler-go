package scheduler

import (
	"testing"
)

// TestMinimalVictimSetMultiDim 验证只抢占“最小必要集合”，
// 并在 GPU 这种独立维度上正确累计回收。
func TestMinimalVictimSetMultiDim(t *testing.T) {
	tenants := []Tenant{
		{ID: "a", Quota: Resource{CPU: 100, Memory: 100000, GPU: 100}},
		{ID: "b", Quota: Resource{CPU: 100, Memory: 100000, GPU: 100}},
	}
	s, _ := newTestScheduler(t, Resource{CPU: 4, Memory: 8192, GPU: 2}, tenants)

	// 三个运行中作业占满：两个 CPU 型（无 GPU），一个 GPU 型。
	mustSubmit(t, s, "cpu-x", "b", 1, Resource{CPU: 1, Memory: 1024})
	mustSubmit(t, s, "cpu-y", "b", 1, Resource{CPU: 1, Memory: 1024})
	mustSubmit(t, s, "gpu-z", "b", 1, Resource{CPU: 2, Memory: 2048, GPU: 2})

	// 需要 2 CPU + 2 GPU：只抢占 gpu-z 即可同时获得 CPU 与 GPU，
	// cpu-x / cpu-y 不应被波及（最小抢占集合）。
	res, err := s.Submit("gpu-hi", "a", 9, Resource{CPU: 2, Memory: 2048, GPU: 2})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if !res.Started {
		t.Fatal("gpu-hi should start after preempting gpu-z")
	}
	snap := s.Snapshot()
	if !contains(snap.RunningIDs, "gpu-hi") || contains(snap.RunningIDs, "gpu-z") {
		t.Fatalf("unexpected running set: %v", snap.RunningIDs)
	}
	if !contains(snap.RunningIDs, "cpu-x") || !contains(snap.RunningIDs, "cpu-y") {
		t.Fatalf("minimal victim set violated, cpu jobs should remain: %v", snap.RunningIDs)
	}
	if len(snap.QueuedIDs) != 1 || snap.QueuedIDs[0] != "gpu-z" {
		t.Fatalf("queue = %v, want only [gpu-z]", snap.QueuedIDs)
	}
	logSnapshot(t, "minimal-victim", s, nil)
}

// TestPreemptedJobRequeuePosition 被抢占作业必须按原始 SubmitSeq
// 回到队列确定性位置：新提交的同优先级作业排在其后（FIFO）。
func TestPreemptedJobRequeuePosition(t *testing.T) {
	tenants := []Tenant{
		{ID: "a", Quota: Resource{CPU: 100}},
		{ID: "b", Quota: Resource{CPU: 100}},
	}
	s, _ := newTestScheduler(t, Resource{CPU: 2}, tenants)

	// seq=1 的低优先级作业运行。
	mustSubmit(t, s, "victim", "b", 1, Resource{CPU: 2})
	// seq=2 高优先级作业抢占 victim。
	mustSubmit(t, s, "boss", "a", 9, Resource{CPU: 2})
	mustState(t, s, "victim", StateQueued)
	mustState(t, s, "boss", StateRunning)

	// seq=3 的新作业（同低优先级）提交，因集群忙排队。
	mustSubmit(t, s, "newcomer", "b", 1, Resource{CPU: 2})
	q := s.Snapshot().QueuedIDs
	if len(q) != 2 || q[0] != "victim" || q[1] != "newcomer" {
		t.Fatalf("requeue position = %v, want [victim newcomer]", q)
	}
	// boss 释放后，victim 必须先于 newcomer 启动（原始提交顺序）。
	if _, err := s.Complete("boss"); err != nil {
		t.Fatalf("complete boss: %v", err)
	}
	mustState(t, s, "victim", StateRunning)
	mustState(t, s, "newcomer", StateQueued)
	logSnapshot(t, "requeue-position", s, nil)
}

// TestCancelRunningReleasesAndSchedules 取消运行中作业会释放资源并推进队首。
func TestCancelRunningReleasesAndSchedules(t *testing.T) {
	tenants := []Tenant{{ID: "a", Quota: Resource{CPU: 100}}}
	s, _ := newTestScheduler(t, Resource{CPU: 2}, tenants)
	mustSubmit(t, s, "r", "a", 1, Resource{CPU: 2})
	mustSubmit(t, s, "w", "a", 1, Resource{CPU: 2})
	if _, err := s.Cancel("r"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	mustState(t, s, "r", StateCanceled)
	mustState(t, s, "w", StateRunning)
	// 取消排队中作业则直接终止，不影响资源账目。
	mustSubmit(t, s, "w2", "a", 1, Resource{CPU: 2})
	if _, err := s.Cancel("w2"); err != nil {
		t.Fatalf("cancel queued: %v", err)
	}
	mustState(t, s, "w2", StateCanceled)
	assertResource(t, s.Snapshot().Used, Resource{CPU: 2}, "used after queued cancel")
}

// TestSameTenantPreemptionWhenNeeded 当跨租户运行者优先级更高、不可抢占时，
// 高优先级作业仍可抢占同租户的低优先级作业（跨租户优先只是平局规则，
// 不是禁止同租户抢占）。
func TestSameTenantPreemptionWhenNeeded(t *testing.T) {
	tenants := []Tenant{
		{ID: "a", Quota: Resource{CPU: 100}},
		{ID: "b", Quota: Resource{CPU: 100}},
	}
	s, _ := newTestScheduler(t, Resource{CPU: 2}, tenants)

	// 一个优先级很高的跨租户运行者（不可被后续作业抢占）。
	mustSubmit(t, s, "b-anchor", "b", 9, Resource{CPU: 1})
	// 同租户低优先级作业占满剩余 1 CPU。
	mustSubmit(t, s, "a-low", "a", 1, Resource{CPU: 1})
	mustState(t, s, "b-anchor", StateRunning)
	mustState(t, s, "a-low", StateRunning)

	// a 租户再来中等优先级作业：b-anchor(9) 不可碰，唯一更低候选是
	// 同租户的 a-low(1)，因此允许同租户抢占。
	res, err := s.Submit("a-mid", "a", 5, Resource{CPU: 1})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if !res.Started {
		t.Fatal("a-mid should preempt same-tenant a-low")
	}
	mustState(t, s, "a-low", StateQueued)
	mustState(t, s, "a-mid", StateRunning)
	mustState(t, s, "b-anchor", StateRunning) // 更高优先级的跨租户作业不受影响
	logSnapshot(t, "same-tenant-preemption", s, nil)
}
