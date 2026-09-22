package scheduler

import (
	"fmt"
	"sync"
	"testing"
)

// TestConcurrentSubmitNoOversell 多 goroutine 并发提交 + 并发 tick：
// 任何时刻集群与租户占用都不得超过上限，作业不得重复调度。
func TestConcurrentSubmitNoOversell(t *testing.T) {
	s := newSched(t, baseConfig(t.TempDir()))

	var wg sync.WaitGroup
	// 20 个唯一作业，每个需求 300/300，集群总量 2000/2048 → 任意时刻最多 6 个运行
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := s.Submit(JobSpec{
				ID: fmt.Sprintf("c%02d", i), Tenant: "beta", Priority: i % 5,
				Request: Resources{CPUMillis: 300, MemoryMB: 300}, DurationTicks: int64(2 + i%3),
			})
			if err != nil {
				t.Errorf("submit c%02d: %v", i, err)
			}
		}(i)
	}

	// 并发达 tick 推进
	var tickWG sync.WaitGroup
	tickWG.Add(1)
	stop := make(chan struct{})
	// 观察器：在并发提交与调度进行中反复读取快照，断言任何时刻不得超卖。
	obsWG := sync.WaitGroup{}
	obsWG.Add(1)
	go func() {
		defer obsWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			snap := s.Snapshot()
			if !snap.Used.FitsIn(snap.Capacity) {
				t.Errorf("并发过程中集群超卖: used=%s capacity=%s", snap.Used, snap.Capacity)
				return
			}
			for _, tn := range snap.Tenants {
				if !tn.Used.FitsIn(tn.Quota) {
					t.Errorf("并发过程中租户 %s 超卖: used=%s quota=%s", tn.Name, tn.Used, tn.Quota)
					return
				}
			}
			// 同一作业不得同时既运行又排队；运行占用之和必须等于 used
			var sum Resources
			seen := map[string]bool{}
			for _, jb := range snap.Jobs {
				if jb.State == StateRunning {
					if seen[jb.ID] {
						t.Errorf("作业 %s 被重复调度", jb.ID)
						return
					}
					seen[jb.ID] = true
					sum = sum.Add(jb.Request)
				}
			}
			if sum != snap.Used {
				t.Errorf("运行占用之和 %s != 快照 used %s（记账泄漏）", sum, snap.Used)
				return
			}
		}
	}()
	go func() {
		defer tickWG.Done()
		for k := 0; k < 40; k++ {
			s.Tick()
		}
		close(stop)
	}()

	wg.Wait()
	tickWG.Wait()
	obsWG.Wait()

	// 充分推进直到全部终态
	for k := 0; k < 100; k++ {
		s.Tick()
		allTerminal := true
		for _, jb := range s.Snapshot().Jobs {
			if !jb.State.Terminal() {
				allTerminal = false
			}
		}
		if allTerminal {
			break
		}
	}

	snap := s.Snapshot()
	t.Logf("并发结束快照: tick=%d 作业数=%d used=%s", snap.Tick, len(snap.Jobs), snap.Used)
	for _, tn := range snap.Tenants {
		if !tn.Used.FitsIn(tn.Quota) {
			t.Fatalf("租户 %s 超卖: used=%s quota=%s", tn.Name, tn.Used, tn.Quota)
		}
	}
	if len(snap.Jobs) != 20 {
		t.Fatalf("作业数=%d, 期望 20", len(snap.Jobs))
	}
	for _, jb := range snap.Jobs {
		if !jb.State.Terminal() {
			t.Fatalf("job %s 未到终态: %s", jb.ID, jb.State)
		}
	}
}

// TestConcurrentDuplicateSubmit 同一 ID 被并发重复提交：恰好受理一次。
func TestConcurrentDuplicateSubmit(t *testing.T) {
	s := newSched(t, baseConfig(t.TempDir()))
	const n = 16
	var wg sync.WaitGroup
	var created int64
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, createdFlag, err := s.Submit(JobSpec{ID: "same", Tenant: "alpha", Priority: 2,
				Request: Resources{CPUMillis: 200, MemoryMB: 200}, DurationTicks: 2})
			if err != nil {
				t.Errorf("dup submit err: %v", err)
				return
			}
			if createdFlag {
				mu.Lock()
				created++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if created != 1 {
		t.Fatalf("同一作业被受理 %d 次, 期望恰好 1 次", created)
	}
	s.Tick()
	snap := s.Snapshot()
	if len(snap.Jobs) != 1 {
		t.Fatalf("作业数=%d, 重复提交产生了占位", len(snap.Jobs))
	}
	if snap.Used != (Resources{CPUMillis: 200, MemoryMB: 200}) {
		t.Fatalf("used=%s, 期望只占一份 200/200", snap.Used)
	}
	t.Logf("16 个并发重复提交：created=%d, used=%s（无重复占位）", created, snap.Used)
}
