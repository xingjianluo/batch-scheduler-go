package scheduler

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// ---- 测试用逻辑时钟与事件序列 ----

// replayOp 是对齐测试中的一步操作。
type replayOp struct {
	kind   RehearsalKind
	id     string
	tenant string
	pri    int
	req    Resource
	at     time.Time
}

func (o replayOp) event() RehearsalEvent {
	switch o.kind {
	case RehearsalSubmit:
		return NewSubmitEvent(o.id, o.tenant, o.pri, o.req)
	case RehearsalComplete:
		return NewCompleteEvent(o.id)
	case RehearsalCancel:
		return NewCancelEvent(o.id)
	default:
		return NewAdvanceEvent(o.at)
	}
}

// replayScenario 构造一段覆盖全部关键路径的事件序列：
// 直接启动、排队、五类拒绝、完成/取消错误、老化、抢占（含凑不齐不抢占）、
// 推进时间触发的抢占、取消运行作业触发级联、终态操作错误、终态 ID 复用。
func replayScenario(t0 time.Time) []replayOp {
	return []replayOp{
		{kind: RehearsalSubmit, id: "a1", tenant: "a", pri: 1, req: Resource{CPU: 2}},
		{kind: RehearsalSubmit, id: "a2", tenant: "a", pri: 1, req: Resource{CPU: 2}},
		{kind: RehearsalSubmit, id: "b1", tenant: "b", pri: 1, req: Resource{CPU: 2}}, // 排队
		{kind: RehearsalSubmit, id: "a1", tenant: "a", pri: 1, req: Resource{CPU: 1}}, // Duplicate
		{kind: RehearsalSubmit, id: "x1", tenant: "nope", pri: 1, req: Resource{CPU: 1}},
		{kind: RehearsalSubmit, id: "bad", tenant: "a", pri: 1, req: Resource{CPU: -1}},
		{kind: RehearsalSubmit, id: "huge", tenant: "a", pri: 1, req: Resource{CPU: 7}},
		{kind: RehearsalSubmit, id: "bquota", tenant: "b", pri: 1, req: Resource{CPU: 5}},
		{kind: RehearsalComplete, id: "ghost"}, // ErrJobNotFound
		{kind: RehearsalCancel, id: "b1"},      // 取消排队
		{kind: RehearsalComplete, id: "b1"},    // 排队作业已取消 -> not found/terminal 路径
		{kind: RehearsalAdvance, at: t0.Add(5 * time.Second)},
		{kind: RehearsalSubmit, id: "boss", tenant: "a", pri: 9, req: Resource{CPU: 3}},
		{kind: RehearsalSubmit, id: "a3", tenant: "a", pri: 8, req: Resource{CPU: 3}},
		{kind: RehearsalAdvance, at: t0.Add(60 * time.Second)},
		{kind: RehearsalCancel, id: "boss"},
		{kind: RehearsalCancel, id: "boss"}, // 已终态
		{kind: RehearsalComplete, id: "a2"},
		{kind: RehearsalComplete, id: "a2"}, // 已完成
		{kind: RehearsalSubmit, id: "a2", tenant: "a", pri: 1, req: Resource{CPU: 1}},
	}
}

func testTenants() []Tenant {
	return []Tenant{
		{ID: "a", Quota: Resource{CPU: 6}},
		{ID: "b", Quota: Resource{CPU: 4}, MaxConcurrent: 2},
	}
}

func testConfig(clk Clock) Config {
	return Config{
		Capacity:     Resource{CPU: 6},
		AgeIncrement: 10,
		AgingPeriod:  10 * time.Second,
		Clock:        clk,
	}
}

func testEvents(t0 time.Time) []RehearsalEvent {
	ops := replayScenario(t0)
	ev := make([]RehearsalEvent, len(ops))
	for i, o := range ops {
		ev[i] = o.event()
	}
	return ev
}

// ---- 真跑对照 ----

type stepOutcome struct {
	index   int
	err     error
	dec     []Decision
	started bool
}

// runReal 在真实调度器上执行同一序列，fakeClock 仅由事件显式推进。
func runReal(t *testing.T, s *Scheduler, clk *fakeClock, t0 time.Time, ops []replayOp) ([]stepOutcome, *Snapshot) {
	t.Helper()
	out := make([]stepOutcome, 0, len(ops))
	for i, o := range ops {
		switch o.kind {
		case RehearsalSubmit:
			res, err := s.Submit(o.id, o.tenant, o.pri, o.req)
			var ds []Decision
			var started bool
			if res != nil {
				ds, started = res.Decisions, res.Started
			}
			out = append(out, stepOutcome{i, err, ds, started})
		case RehearsalComplete:
			d, err := s.Complete(o.id)
			var ds []Decision
			if d != nil {
				ds = []Decision{*d}
			}
			out = append(out, stepOutcome{i, err, ds, false})
		case RehearsalCancel:
			d, err := s.Cancel(o.id)
			var ds []Decision
			if d != nil {
				ds = []Decision{*d}
			}
			out = append(out, stepOutcome{i, err, ds, false})
		case RehearsalAdvance:
			clk.t = o.at
			ds, err := s.Tick(o.at)
			out = append(out, stepOutcome{i, err, ds, false})
		}
	}
	return out, s.Snapshot()
}

// ---- 断言辅助 ----

func rejectKey(err error) (int, string, bool) {
	var rj *RejectError
	if errors.As(err, &rj) {
		return int(rj.Kind), rj.Error(), true
	}
	return -1, "", false
}

func errorsEqual(a, b error) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.Error() != b.Error() {
		return false
	}
	ka, _, ra := rejectKey(a)
	kb, _, rb := rejectKey(b)
	if ra != rb {
		return false
	}
	return ka == kb
}

func decisionsEqual(a, b []Decision) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.JobID != y.JobID || x.Action != y.Action || x.Reason != y.Reason ||
			x.JobState != y.JobState || x.Used != y.Used || x.Capacity != y.Capacity {
			return false
		}
		if !equalStrings(x.VictimIDs, y.VictimIDs) || !x.Time.Equal(y.Time) {
			return false
		}
	}
	return true
}

func dumpDecisions(ds []Decision) string {
	out := "["
	for i, d := range ds {
		if i > 0 {
			out += ", "
		}
		out += fmt.Sprintf("{%s %s %s used=%+v victims=%v}",
			d.JobID, d.Action, d.JobState, d.Used, d.VictimIDs)
	}
	return out + "]"
}

func assertOutcomesEqual(t *testing.T, real []stepOutcome, reps []RehearsalStepReport) {
	t.Helper()
	if len(real) != len(reps) {
		t.Fatalf("step count: real=%d rehearsal=%d", len(real), len(reps))
	}
	for i := range real {
		ro, rp := real[i], reps[i]
		if !errorsEqual(ro.err, rp.Err) {
			t.Fatalf("step %d %v err mismatch:\n real=%q\n rehe=%q",
				i, rp.Event, fmtReject(ro.err), fmtReject(rp.Err))
		}
		if !decisionsEqual(ro.dec, rp.Decisions) {
			t.Fatalf("step %d %v decisions mismatch:\n real=%s\n rehe=%s",
				i, rp.Event, dumpDecisions(ro.dec), dumpDecisions(rp.Decisions))
		}
		rhStarted := rp.Submit != nil && rp.Submit.Started
		if ro.started != rhStarted {
			t.Fatalf("step %d started mismatch: real=%v rehe=%v", i, ro.started, rhStarted)
		}
	}
}

func assertSnapshotsDeep(t *testing.T, want, got *Snapshot, what string) {
	t.Helper()
	if want.Capacity != got.Capacity || want.Used != got.Used {
		t.Fatalf("%s capacity/used real=%+v/%+v rehe=%+v/%+v", what,
			want.Capacity, want.Used, got.Capacity, got.Used)
	}
	if !equalStrings(want.QueuedIDs, got.QueuedIDs) ||
		!equalStrings(want.RunningIDs, got.RunningIDs) {
		t.Fatalf("%s ids real q=%v r=%v rehe q=%v r=%v", what,
			want.QueuedIDs, want.RunningIDs, got.QueuedIDs, got.RunningIDs)
	}
	if len(want.Jobs) != len(got.Jobs) {
		t.Fatalf("%s job count real=%d rehe=%d", what, len(want.Jobs), len(got.Jobs))
	}
	for i := range want.Jobs {
		if want.Jobs[i] != got.Jobs[i] {
			t.Fatalf("%s job[%d]\n real=%+v\n rehe=%+v", what, i, want.Jobs[i], got.Jobs[i])
		}
	}
	if !reflect.DeepEqual(want.TenantUsed, got.TenantUsed) {
		t.Fatalf("%s tenantUsed real=%+v rehe=%+v", what, want.TenantUsed, got.TenantUsed)
	}
}

func assertInvariants(t *testing.T, snap *Snapshot, tenants map[string]Tenant, phase string) {
	t.Helper()
	if !snap.Capacity.Fits(snap.Used) {
		t.Fatalf("%s: cluster oversold used=%+v cap=%+v", phase, snap.Used, snap.Capacity)
	}
	seen := map[string]bool{}
	var sum Resource
	runningCount := map[string]int{}
	jobByID := map[string]Job{}
	for _, j := range snap.Jobs {
		if seen[j.ID] {
			t.Fatalf("%s: duplicated job %s", phase, j.ID)
		}
		seen[j.ID] = true
		jobByID[j.ID] = j
		if j.State == StateRunning {
			sum = sum.Add(j.Request)
			runningCount[j.TenantID]++
		}
	}
	if sum != snap.Used {
		t.Fatalf("%s: cluster accounts inconsistent used=%+v sum=%+v", phase, snap.Used, sum)
	}
	for tid, used := range snap.TenantUsed {
		var tsum Resource
		for _, j := range snap.Jobs {
			if j.TenantID == tid && j.State == StateRunning {
				tsum = tsum.Add(j.Request)
			}
		}
		if tsum != used {
			t.Fatalf("%s: tenant %s accounts inconsistent used=%+v sum=%+v", phase, tid, used, tsum)
		}
		if q := tenants[tid].Quota; !q.Fits(used) {
			t.Fatalf("%s: tenant %s over quota used=%+v quota=%+v", phase, tid, used, q)
		}
		if mc := tenants[tid].MaxConcurrent; mc > 0 && runningCount[tid] > mc {
			t.Fatalf("%s: tenant %s exceeds max concurrent %d>%d", phase, tid, runningCount[tid], mc)
		}
	}
	if len(snap.QueuedIDs) != len(uniqueNonEmpty(snap.QueuedIDs)) {
		t.Fatalf("%s: duplicated queued ids %v", phase, snap.QueuedIDs)
	}
	for _, id := range snap.QueuedIDs {
		if jobByID[id].State != StateQueued {
			t.Fatalf("%s: queued id %s not Queued", phase, id)
		}
	}
	for _, id := range snap.RunningIDs {
		if jobByID[id].State != StateRunning {
			t.Fatalf("%s: running id %s not Running", phase, id)
		}
	}
}

func uniqueNonEmpty(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func tenantMap(ts []Tenant) map[string]Tenant {
	m := map[string]Tenant{}
	for _, x := range ts {
		m[x.ID] = x
	}
	return m
}

func containsJob(jobs []Job, id string) bool {
	for _, j := range jobs {
		if j.ID == id {
			return true
		}
	}
	return false
}

// ---- 测试 1：从运行中的真实调度器 fork，推演与真跑逐项对齐，且互不干扰 ----

func TestRehearsalParityFromLive(t *testing.T) {
	t0 := time.Date(2026, 9, 30, 8, 0, 0, 0, time.UTC)
	tenants := testTenants()
	ops := replayScenario(t0)

	// 第一台真实调度器：仅用于验证 fork 的双向隔离。
	src := newFakeClock()
	src.t = t0
	live, err := New(testConfig(src), tenants)
	if err != nil {
		t.Fatalf("New live: %v", err)
	}
	t.Cleanup(func() { _ = live.Close() })
	mustSubmit(t, live, "seed-run", "a", 1, Resource{CPU: 1})
	mustSubmit(t, live, "seed-queue", "a", 1, Resource{CPU: 1})

	rhIso, err := NewRehearsalFromLive(live, "")
	if err != nil {
		t.Fatalf("fork iso: %v", err)
	}
	t.Cleanup(func() { _ = rhIso.Close() })
	mustSubmit(t, live, "real-only", "a", 1, Resource{CPU: 1})
	if snap := rhIso.Snapshot(); containsJob(snap.Jobs, "real-only") {
		t.Fatalf("live mutation leaked into rehearsal")
	}
	if _, err := rhIso.Apply(NewSubmitEvent("rehe-only", "a", 1, Resource{CPU: 1})); err != nil {
		t.Fatalf("rehe apply: %v", err)
	}
	if _, err := live.Get("rehe-only"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("rehearsal mutation leaked into live: %v", err)
	}
	// fork 时刻逻辑时间也必须取自源调度器，而非墙钟。
	if !rhIso.Now().Equal(t0) {
		t.Fatalf("rehearsal start time = %v, want %v", rhIso.Now(), t0)
	}

	// 第二台：从同一播种状态重新 fork 一台干净推演，与干净真跑对照器逐项对齐。
	clk := newFakeClock()
	clk.t = t0
	control, err := New(testConfig(clk), tenants)
	if err != nil {
		t.Fatalf("New control: %v", err)
	}
	t.Cleanup(func() { _ = control.Close() })
	mustSubmit(t, control, "seed-run", "a", 1, Resource{CPU: 1})
	mustSubmit(t, control, "seed-queue", "a", 1, Resource{CPU: 1})

	rh, err := NewRehearsalFromLive(control, "")
	if err != nil {
		t.Fatalf("fork: %v", err)
	}
	t.Cleanup(func() { _ = rh.Close() })

	reps, ferr := rh.ApplyAll(testEvents(t0))
	if ferr != nil {
		t.Fatalf("ApplyAll: %v", ferr)
	}
	realOut, finalSnap := runReal(t, control, clk, t0, ops)

	assertOutcomesEqual(t, realOut, reps)
	assertSnapshotsDeep(t, finalSnap, rh.Snapshot(), "final")
	tm := tenantMap(tenants)
	for _, rp := range reps {
		assertInvariants(t, rp.Snapshot, tm, "step "+itoa(rp.Index))
	}
	t.Logf("live-fork parity verified over %d steps: %s", len(reps), dumpSnap(rh.Snapshot()))
}

// ---- 测试 2：从已保存状态点出发，推演与真跑逐项对齐；源状态文件只读 ----

func TestRehearsalParityFromStateFile(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	t0 := time.Date(2026, 9, 30, 9, 0, 0, 0, time.UTC)
	tenants := testTenants()
	ops := replayScenario(t0)

	clk := newFakeClock()
	clk.t = t0
	seed := newPersistentScheduler(t, statePath, clk, Resource{CPU: 6}, tenants)
	mustSubmit(t, seed, "seed-run", "a", 1, Resource{CPU: 2})
	mustSubmit(t, seed, "seed-queue", "b", 1, Resource{CPU: 2})
	if err := seed.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}
	origBytes, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}

	rh, err := NewRehearsalFromStateFile(statePath, "", testConfig(nil), t0)
	if err != nil {
		t.Fatalf("NewRehearsalFromStateFile: %v", err)
	}
	t.Cleanup(func() { _ = rh.Close() })
	reps, ferr := rh.ApplyAll(testEvents(t0))
	if ferr != nil {
		t.Fatalf("ApplyAll: %v", ferr)
	}

	// 源状态文件必须保持字节级不变（只读）。
	afterBytes, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(origBytes, afterBytes) {
		t.Fatalf("source state file was modified by rehearsal")
	}

	// 真跑对照：从同一状态文件恢复，逻辑时钟同样仅由事件推进。
	clkCtrl := newFakeClock()
	clkCtrl.t = t0
	control := newPersistentScheduler(t, statePath, clkCtrl, Resource{CPU: 6}, tenants)
	t.Cleanup(func() { _ = control.Close() })
	realOut, finalSnap := runReal(t, control, clkCtrl, t0, ops)

	assertOutcomesEqual(t, realOut, reps)
	assertSnapshotsDeep(t, finalSnap, rh.Snapshot(), "state-file final")
	tm := tenantMap(tenants)
	for _, rp := range reps {
		assertInvariants(t, rp.Snapshot, tm, "step "+itoa(rp.Index))
	}
	t.Logf("state-file parity verified over %d steps: %s", len(reps), dumpSnap(rh.Snapshot()))
}
