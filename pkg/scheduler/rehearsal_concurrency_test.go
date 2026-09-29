package scheduler

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestRehearsalConcurrentApply 并发向同一轮推演提交不同事件：
// 每步原子串行生效，最终不超卖、无重复作业，且可被完整重放复现。
func TestRehearsalConcurrentApply(t *testing.T) {
	t0 := time.Date(2026, 9, 30, 18, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	sp := filepath.Join(dir, "state.json")
	cp := filepath.Join(dir, "r.json")
	seedStateFile(t, sp, t0)

	rh, err := NewRehearsalFromStateFile(sp, cp, testConfig(nil), t0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rh.Close() })

	const n = 40
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, e := rh.Apply(NewSubmitEvent("c-"+itoa(i), "a", i%4, Resource{CPU: 1}))
			errs <- e
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatalf("concurrent apply: %v", e)
		}
	}

	snap := rh.Snapshot()
	assertInvariants(t, snap, tenantMap(testTenants()), "concurrent rehearsal")
	if len(snap.Jobs) != n+2 { // 含 2 个种子作业
		t.Fatalf("job count = %d, want %d", len(snap.Jobs), n+2)
	}
	// 序号必须唯一且连续分配过（每个作业一个不同 SubmitSeq）。
	seq := map[int64]bool{}
	for _, j := range snap.Jobs {
		if seq[j.SubmitSeq] {
			t.Fatalf("duplicate SubmitSeq %d", j.SubmitSeq)
		}
		seq[j.SubmitSeq] = true
	}
	if rh.Applied() != n {
		t.Fatalf("applied = %d, want %d", rh.Applied(), n)
	}

	// 重开存档：并发结果必须完整落盘并自洽。
	rh2, err := OpenRehearsal(cp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rh2.Close() })
	assertSnapshotsDeep(t, snap, rh2.Snapshot(), "concurrent after reopen")
	if rh2.Applied() != n {
		t.Fatalf("reopen applied = %d, want %d", rh2.Applied(), n)
	}
}
