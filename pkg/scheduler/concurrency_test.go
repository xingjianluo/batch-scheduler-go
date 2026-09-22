package scheduler

import (
	"fmt"
	"sync"
	"testing"
)

// TestConcurrentSubmitNoOversell 多租户高并发提交后，任意时刻集群与各租户
// 资源占用都不得超过上限；且每个作业只被调度一次（运行 ID 无重复）。
func TestConcurrentSubmitNoOversell(t *testing.T) {
	tenants := []Tenant{
		{ID: "a", Quota: Resource{CPU: 6, Memory: 6144, GPU: 2}},
		{ID: "b", Quota: Resource{CPU: 4}},
	}
	s, _ := newTestScheduler(t, Resource{CPU: 10, Memory: 10240, GPU: 4}, tenants)

	const perTenant = 60
	var wg sync.WaitGroup
	errs := make(chan error, 2*perTenant)
	for i := 0; i < perTenant; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("a-%03d", i)
			if _, err := s.Submit(id, "a", i%5, Resource{CPU: 1, Memory: 1024, GPU: (i % 2)}); err != nil {
				errs <- err
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("b-%03d", i)
			if _, err := s.Submit(id, "b", i%3, Resource{CPU: 1}); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("unexpected submit error: %v", err)
	}

	snap := s.Snapshot()
	t.Logf("post-concurrency: running=%d queued=%d used=%+v tenantUsed=%v",
		len(snap.RunningIDs), len(snap.QueuedIDs), snap.Used, snap.TenantUsed)

	// 1) 集群维度绝不超卖。
	if !snap.Capacity.Fits(snap.Used) {
		t.Fatalf("cluster oversold: used=%+v capacity=%+v", snap.Used, snap.Capacity)
	}
	// 2) 各租户维度绝不超卖。
	for _, tn := range snap.Tenants {
		u := snap.TenantUsed[tn.ID]
		if !tn.Quota.Fits(u) {
			t.Fatalf("tenant %s oversold: used=%+v quota=%+v", tn.ID, u, tn.Quota)
		}
	}
	// 3) 运行中作业 ID 唯一，且每个作业恰好出现一次（无重复调度）。
	seen := map[string]int{}
	for _, id := range snap.RunningIDs {
		seen[id]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %s scheduled %d times", id, n)
		}
	}
	if len(snap.Jobs) != 2*perTenant {
		t.Fatalf("accepted jobs = %d, want %d", len(snap.Jobs), 2*perTenant)
	}
	// 4) 资源账目与运行中作业的需求之和逐维一致。
	var sum Resource
	for _, j := range snap.Jobs {
		if j.State == StateRunning {
			sum = sum.Add(j.Request)
		}
	}
	if sum != snap.Used {
		t.Fatalf("accounted used=%+v != sum of running requests=%+v", snap.Used, sum)
	}
	logSnapshot(t, "concurrency-final", s, nil)
}

// TestConcurrentDuplicateSubmit 多个 goroutine 同时提交同一作业 ID：
// 恰好一个成功，其余全部得到 DuplicateJob 拒绝，且只有一份占位。
func TestConcurrentDuplicateSubmit(t *testing.T) {
	tenants := []Tenant{{ID: "a", Quota: Resource{CPU: 100}}}
	s, _ := newTestScheduler(t, Resource{CPU: 100}, tenants)

	const n = 32
	var wg sync.WaitGroup
	var ok, dup int
	var mu sync.Mutex
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			res, err := s.Submit("same-id", "a", 1, Resource{CPU: 1})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil && res != nil:
				ok++
			case IsReject(err, RejectDuplicateJob):
				dup++
			default:
				t.Errorf("unexpected err: %v", err)
			}
		}()
	}
	wg.Wait()
	t.Logf("duplicate race: accepted=%d duplicate-rejected=%d/%d", ok, dup, n)
	if ok != 1 {
		t.Fatalf("accepted = %d, want exactly 1", ok)
	}
	if int(ok+dup) != n {
		t.Fatalf("accounted outcomes = %d, want %d", ok+dup, n)
	}
	snap := s.Snapshot()
	if len(snap.Jobs) != 1 || snap.Used != (Resource{CPU: 1}) {
		t.Fatalf("unexpected state: jobs=%d used=%+v", len(snap.Jobs), snap.Used)
	}
}
