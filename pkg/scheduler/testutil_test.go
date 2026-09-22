package scheduler

import (
	"fmt"
	"testing"
	"time"
)

// fakeClock 是可手动推进的确定性时钟。
type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time { return c.t }

func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 9, 22, 8, 0, 0, 0, time.UTC)}
}

// logSnapshot 打印关键输入、资源快照与判定依据（测试日志要求）。
func logSnapshot(t *testing.T, phase string, s *Scheduler, decisions []Decision) {
	t.Helper()
	snap := s.Snapshot()
	t.Logf("=== phase=%s time=%s capacity=%+v used=%+v queued=%v running=%v tenantUsed=%v",
		phase, snap.Time.Format(time.RFC3339), snap.Capacity, snap.Used,
		snap.QueuedIDs, snap.RunningIDs, snap.TenantUsed)
	for _, d := range decisions {
		if len(d.VictimIDs) > 0 {
			t.Logf("    decision: job=%s action=%-12s state=%-9s used=%+v victims=%v reason=%q",
				d.JobID, d.Action, d.JobState, d.Used, d.VictimIDs, d.Reason)
		} else {
			t.Logf("    decision: job=%s action=%-12s state=%-9s used=%+v reason=%q",
				d.JobID, d.Action, d.JobState, d.Used, d.Reason)
		}
	}
}

func mustState(t *testing.T, s *Scheduler, id string, want JobState) {
	t.Helper()
	j, err := s.Get(id)
	if err != nil {
		t.Fatalf("Get(%s): %v", id, err)
	}
	if j.State != want {
		t.Fatalf("job %s state = %s, want %s", id, j.State, want)
	}
}

func assertResource(t *testing.T, got, want Resource, what string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %+v, want %+v", what, got, want)
	}
}

func newTestScheduler(t *testing.T, cap_ Resource, tenants []Tenant) (*Scheduler, *fakeClock) {
	t.Helper()
	clk := newFakeClock()
	s, err := New(Config{
		Capacity:     cap_,
		AgeIncrement: 10,
		AgingPeriod:  10 * time.Second,
		Clock:        clk,
	}, tenants)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, clk
}

func fmtReject(err error) string {
	if err == nil {
		return "<nil>"
	}
	if r, ok := err.(*RejectError); ok {
		return fmt.Sprintf("%s:%s", RejectKindName(r.Kind), r.Reason)
	}
	return err.Error()
}
