package scheduler

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// baselineRehearsal 一口气跑完整个事件序列，作为所有“中断续跑”对照基准。
func baselineRehearsal(t *testing.T, statePath, cpPath string, t0 time.Time) (*Rehearsal, []RehearsalStepReport) {
	t.Helper()
	rh, err := NewRehearsalFromStateFile(statePath, cpPath, testConfig(nil), t0)
	if err != nil {
		t.Fatalf("baseline new: %v", err)
	}
	reps, err := rh.ApplyAll(testEvents(t0))
	if err != nil {
		t.Fatalf("baseline apply: %v", err)
	}
	return rh, reps
}

// TestRehearsalResumeNoDrift 在每个可能的中断点重启推演后续跑，
// 最终快照、历史步骤报告必须与一口气跑完逐项一致。
func TestRehearsalResumeNoDrift(t *testing.T) {
	t0 := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	events := testEvents(t0)
	tm := tenantMap(testTenants())

	for cutoff := 0; cutoff <= len(events); cutoff++ {
		cutoff := cutoff
		name := "cutoff-" + itoa(cutoff)
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			statePath := filepath.Join(dir, "state.json")
			cpPath := filepath.Join(dir, "rehearsal.json")
			seedStateFile(t, statePath, t0)

			// 基准：一口气跑完。
			baseDir := t.TempDir()
			baseCp := filepath.Join(baseDir, "rehearsal.json")
			baseState := filepath.Join(t.TempDir(), "state.json")
			seedStateFile(t, baseState, t0)
			baseRh, baseReps := baselineRehearsal(t, baseState, baseCp, t0)
			t.Cleanup(func() { _ = baseRh.Close() })

			// 中断跑：跑到 cutoff（含 0 步：刚创建就“被杀”）。
			rh, err := NewRehearsalFromStateFile(statePath, cpPath, testConfig(nil), t0)
			if err != nil {
				t.Fatalf("new: %v", err)
			}
			for i := 0; i < cutoff; i++ {
				if _, err := rh.Apply(events[i]); err != nil {
					t.Fatalf("apply %d: %v", i, err)
				}
			}
			// 模拟进程被干掉：不 Close，仅依赖每步原子存档。
			rh = nil
			_ = rh

			// 重新起来，从存档续跑剩余事件。
			cont, err := OpenRehearsal(cpPath)
			if err != nil {
				t.Fatalf("open at cutoff %d: %v", cutoff, err)
			}
			if cont.Applied() != cutoff {
				t.Fatalf("applied after reopen = %d, want %d", cont.Applied(), cutoff)
			}
			if !cont.Now().Equal(logicalTimeAfter(t0, events[:cutoff])) {
				t.Fatalf("logical time after reopen = %v, want %v",
					cont.Now(), logicalTimeAfter(t0, events[:cutoff]))
			}
			for i := cutoff; i < len(events); i++ {
				if _, err := cont.Apply(events[i]); err != nil {
					t.Fatalf("resume apply %d: %v", i, err)
				}
			}

			// 最终状态逐项一致。
			assertSnapshotsDeep(t, baseRh.Snapshot(), cont.Snapshot(), "resume final")
			// 历史步骤报告逐项一致（错误、裁决、时间）。
			gotReps := cont.Steps()
			if len(gotReps) != len(baseReps) {
				t.Fatalf("steps len = %d, want %d", len(gotReps), len(baseReps))
			}
			for i := range baseReps {
				if !reportsEqualForHistory(baseReps[i], gotReps[i]) {
					t.Fatalf("step %d differs after resume:\n base=%v\n cont=%v",
						i, dumpStep(baseReps[i]), dumpStep(gotReps[i]))
				}
			}
			assertInvariants(t, cont.Snapshot(), tm, "resume")
			t.Logf("cutoff=%d: 中断续跑 %d 步历史报告（含每步状态快照/受理结果/失败原因）与最终状态均逐项一致",
				cutoff, len(gotReps))
			_ = cont.Close()
		})
	}
}

// logicalTimeAfter 返回执行前缀事件后的逻辑时间。
func logicalTimeAfter(t0 time.Time, evs []RehearsalEvent) time.Time {
	now := t0
	for _, e := range evs {
		if e.Kind == RehearsalAdvance {
			now = e.At
		}
	}
	return now
}

// reportsEqualForHistory 严格逐项比较两份步骤报告：
// 事件、逻辑时间、完整裁决列表、提交受理结果（含受理/拒绝的 nil 性、
// Started、受理裁决、作业副本）、当时状态快照与错误，全部一致才算相等。
func reportsEqualForHistory(a, b RehearsalStepReport) bool {
	if a.Index != b.Index {
		return false
	}
	if a.Event != b.Event {
		return false
	}
	if !a.Time.Equal(b.Time) {
		return false
	}
	if !decisionsEqual(a.Decisions, b.Decisions) {
		return false
	}
	if !submitResultsEqual(a.Submit, b.Submit) {
		return false
	}
	if !snapshotsDeepEqual(a.Snapshot, b.Snapshot) {
		return false
	}
	return errorsEqual(a.Err, b.Err)
}

// submitResultsEqual 比较提交受理结果：nil 性（受理 vs 拒绝）、Started、
// 受理裁决、完整裁决列表与作业副本逐项一致。
func submitResultsEqual(a, b *SubmitResult) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.Started != b.Started {
		return false
	}
	if (a.Job == nil) != (b.Job == nil) {
		return false
	}
	if a.Job != nil && *a.Job != *b.Job {
		return false
	}
	if !decisionsEqual([]Decision{a.Decision}, []Decision{b.Decision}) {
		return false
	}
	return decisionsEqual(a.Decisions, b.Decisions)
}

// snapshotsDeepEqual 深比较两份状态快照（含快照时间与租户列表）。
func snapshotsDeepEqual(a, b *Snapshot) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if !a.Time.Equal(b.Time) || a.Capacity != b.Capacity || a.Used != b.Used {
		return false
	}
	if !equalStrings(a.QueuedIDs, b.QueuedIDs) || !equalStrings(a.RunningIDs, b.RunningIDs) {
		return false
	}
	if len(a.Jobs) != len(b.Jobs) || len(a.Tenants) != len(b.Tenants) {
		return false
	}
	for i := range a.Jobs {
		if a.Jobs[i] != b.Jobs[i] {
			return false
		}
	}
	for i := range a.Tenants {
		if a.Tenants[i] != b.Tenants[i] {
			return false
		}
	}
	return reflect.DeepEqual(a.TenantUsed, b.TenantUsed)
}

func dumpStep(r RehearsalStepReport) string {
	sub := false
	if r.Submit != nil {
		sub = r.Submit.Started
	}
	return "idx=" + itoa(r.Index) + " time=" + r.Time.Format(time.RFC3339) +
		" started=" + itoaBool(sub) + " err=" + fmtReject(r.Err) +
		" dec=" + dumpDecisions(r.Decisions)
}

func itoaBool(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// seedStateFile 在 path 上制造一个真实调度器状态点。
func seedStateFile(t *testing.T, path string, t0 time.Time) {
	t.Helper()
	clk := newFakeClock()
	clk.t = t0
	s := newPersistentScheduler(t, path, clk, Resource{CPU: 6}, testTenants())
	mustSubmit(t, s, "seed-run", "a", 1, Resource{CPU: 2})
	mustSubmit(t, s, "seed-queue", "b", 1, Resource{CPU: 2})
	if err := s.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}
}

// ---- 存档失败时的对外语义 ----

// failingSaveRehearsal 构造一个可注入存档失败的推演器（白盒测试）。
func failingSaveRehearsal(t *testing.T, cpPath string, fail *bool, t0 time.Time) *Rehearsal {
	t.Helper()
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	seedStateFile(t, statePath, t0)
	cfg := testConfig(nil)
	store, err := newFileStore(statePath)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	// 先正常创建（写出 v0 存档），再替换保存函数为可失败版本。
	r, err := newRehearsal(cpPath, cfg, t0, st, true)
	if err != nil {
		t.Fatalf("newRehearsal: %v", err)
	}
	r.saveFunc = func(cp *rehearsalCheckpoint) error {
		if *fail {
			return errInjectedSave
		}
		return saveRehearsalCheckpoint(cpPath, cp)
	}
	return r
}

var errInjectedSave = errors.New("injected checkpoint save failure")

// TestRehearsalSaveFailureAtomic 演算成功但存档失败时：
// 对外（内存与重启后的磁盘）都看不到该步；原样重试幂等、不多占资源。
func TestRehearsalSaveFailureAtomic(t *testing.T) {
	t0 := time.Date(2026, 9, 30, 11, 0, 0, 0, time.UTC)
	events := testEvents(t0)
	tm := tenantMap(testTenants())

	for failAt := 1; failAt <= len(events); failAt++ {
		failAt := failAt
		t.Run("fail-at-"+itoa(failAt), func(t *testing.T) {
			dir := t.TempDir()
			cpPath := filepath.Join(dir, "rehearsal.json")
			fail := false
			r := failingSaveRehearsal(t, cpPath, &fail, t0)

			// 正常执行到 failAt-1。
			for i := 0; i < failAt-1; i++ {
				if _, err := r.Apply(events[i]); err != nil {
					t.Fatalf("apply %d: %v", i, err)
				}
			}
			beforeSnap := r.Snapshot()
			beforeApplied := r.Applied()
			beforeNow := r.Now()

			// 第 failAt 步存档失败。
			fail = true
			rep, err := r.Apply(events[failAt-1])
			if !errors.Is(err, errInjectedSave) {
				t.Fatalf("want injected save error, got %v (rep=%v)", err, rep)
			}
			if rep != nil {
				t.Fatalf("failed step must not return a report")
			}
			// 内存状态原地不动。
			if r.Applied() != beforeApplied {
				t.Fatalf("applied changed after failed save: %d -> %d", beforeApplied, r.Applied())
			}
			if !r.Now().Equal(beforeNow) {
				t.Fatalf("logical time changed after failed save: %v -> %v", beforeNow, r.Now())
			}
			assertSnapshotsDeep(t, beforeSnap, r.Snapshot(), "post-fail memory")
			assertInvariants(t, r.Snapshot(), tm, "post-fail memory")

			// “被杀重启”：磁盘上也看不到失败步。
			r2, err := OpenRehearsal(cpPath)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			if r2.Applied() != beforeApplied {
				t.Fatalf("disk applied = %d, want %d (failed step must not be visible)",
					r2.Applied(), beforeApplied)
			}
			assertSnapshotsDeep(t, beforeSnap, r2.Snapshot(), "post-fail disk")

			// 原样重试同一事件：成功且只生效一次（不多占资源、不错位）。
			fail = false
			rep2, err := r2.Apply(events[failAt-1])
			if err != nil {
				t.Fatalf("retry: %v", err)
			}
			if rep2.Index != beforeApplied {
				t.Fatalf("retried step index = %d, want %d", rep2.Index, beforeApplied)
			}

			// 跑完剩余全部事件，与一口气基准逐项一致。
			baseDir := t.TempDir()
			seed := filepath.Join(t.TempDir(), "state.json")
			seedStateFile(t, seed, t0)
			baseRh, baseReps := baselineRehearsal(t, seed, filepath.Join(baseDir, "b.json"), t0)
			t.Cleanup(func() { _ = baseRh.Close() })
			for i := failAt; i < len(events); i++ {
				if _, err := r2.Apply(events[i]); err != nil {
					t.Fatalf("post-retry apply %d: %v", i, err)
				}
			}
			assertSnapshotsDeep(t, baseRh.Snapshot(), r2.Snapshot(), "after retry")
			got := r2.Steps()
			for i := range baseReps {
				if !reportsEqualForHistory(baseReps[i], got[i]) {
					t.Fatalf("step %d differs after save-failure retry", i)
				}
			}
			assertInvariants(t, r2.Snapshot(), tm, "after retry")
			_ = r2.Close()
		})
	}
}

// TestRehearsalSaveFailureRepeated 同一步连续失败多次、反复重试，
// 最终仍只生效一次，结论不漂移。
func TestRehearsalSaveFailureRepeated(t *testing.T) {
	t0 := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "rehearsal.json")
	fail := false
	r := failingSaveRehearsal(t, cpPath, &fail, t0)
	ev := testEvents(t0)
	if _, err := r.Apply(ev[0]); err != nil {
		t.Fatal(err)
	}
	snap0 := r.Snapshot()

	// Advance 存档失败：逻辑时钟也必须原地不动（候选时钟与当前时钟独立）。
	advance := NewAdvanceEvent(t0.Add(120 * time.Second))
	now0 := r.Now()
	fail = true
	for i := 0; i < 3; i++ {
		if _, err := r.Apply(advance); !errors.Is(err, errInjectedSave) {
			t.Fatalf("advance attempt %d: want injected error, got %v", i, err)
		}
	}
	if !r.Now().Equal(now0) {
		t.Fatalf("failed Advance polluted logical time: now=%v want=%v", r.Now(), now0)
	}
	assertSnapshotsDeep(t, snap0, r.Snapshot(), "after failed advances")
	fail = false
	if _, err := r.Apply(advance); err != nil {
		t.Fatalf("advance retry: %v", err)
	}
	if !r.Now().Equal(t0.Add(120 * time.Second)) {
		t.Fatalf("logical time after advance retry = %v", r.Now())
	}

	fail = true
	// ev[1] 是一个重复提交（DuplicateJob），业务结果不改变状态；
	// 5 次存档失败期间步数与状态必须原地不动。
	snapBeforeDup := r.Snapshot()
	for i := 0; i < 5; i++ {
		if _, err := r.Apply(ev[1]); !errors.Is(err, errInjectedSave) {
			t.Fatalf("attempt %d: want injected error, got %v", i, err)
		}
	}
	assertSnapshotsDeep(t, snapBeforeDup, r.Snapshot(), "after 5 failures")
	if r.Applied() != 2 { // ev[0] + 成功重试的 Advance
		t.Fatalf("applied = %d, want 2", r.Applied())
	}
	fail = false
	if _, err := r.Apply(ev[1]); err != nil {
		t.Fatalf("retry after failures: %v", err)
	}
	if r.Applied() != 3 {
		t.Fatalf("applied = %d, want 3", r.Applied())
	}
	assertInvariants(t, r.Snapshot(), tenantMap(testTenants()), "repeated-fail")
}

// ---- 逻辑时间语义 ----

// TestRehearsalLogicalTimeDeterminism 同一段事件：
//  1. 瞬间跑完与带真实 wall-clock 停顿分批跑完结论一致；
//  2. 重放多次结论一致；
//  3. 不推进逻辑时间时真实等待不影响老化结论。
func TestRehearsalLogicalTimeDeterminism(t *testing.T) {
	t0 := time.Date(2026, 9, 30, 13, 0, 0, 0, time.UTC)
	events := testEvents(t0)

	run := func(sleepBetween bool) *Snapshot {
		dir := t.TempDir()
		statePath := filepath.Join(dir, "state.json")
		seedStateFile(t, statePath, t0)
		rh, err := NewRehearsalFromStateFile(statePath, "", testConfig(nil), t0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = rh.Close() })
		for i, ev := range events {
			if sleepBetween && i%3 == 0 {
				time.Sleep(5 * time.Millisecond) // 真实时间流逝
			}
			if _, err := rh.Apply(ev); err != nil {
				t.Fatalf("apply %d: %v", i, err)
			}
		}
		return rh.Snapshot()
	}

	fast := run(false)
	var slow *Snapshot
	for k := 0; k < 3; k++ {
		slow = run(true)
		if !reflectDeepSnapshot(fast, slow) {
			t.Fatalf("run %d: wall-clock timing changed logical outcome", k)
		}
	}

	// 不发 Advance，即便真实等待，排队作业也不会因“老化”抢占。
	dir := t.TempDir()
	sp := filepath.Join(dir, "s.json")
	seedStateFile(t, sp, t0)
	rh, _ := NewRehearsalFromStateFile(sp, "", testConfig(nil), t0)
	t.Cleanup(func() { _ = rh.Close() })
	// seed-run 占 2 CPU；再提交一个低优先级大作业排队，真实睡 50ms（>老化周期），
	// 但逻辑时间不动，提交同优先级作业不应发生老化相关抢占。
	_, err := rh.Apply(NewSubmitEvent("low", "a", 0, Resource{CPU: 4}))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	reps, err := rh.Apply(NewSubmitEvent("peer", "a", 0, Resource{CPU: 2}))
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range reps.Decisions {
		if d.Action == "preempt" {
			t.Fatalf("preempt happened without logical time advance: %+v", d)
		}
	}
	j, gerr := getJob(rh, "low")
	if gerr != nil || j.State != StateQueued {
		t.Fatalf("low job state = %v err=%v, want still Queued (no aging without logical advance)",
			j, gerr)
	}
}

func getJob(r *Rehearsal, id string) (*Job, error) {
	return r.sched.Get(id)
}

func reflectDeepSnapshot(a, b *Snapshot) bool {
	if a.Used != b.Used || !equalStrings(a.QueuedIDs, b.QueuedIDs) ||
		!equalStrings(a.RunningIDs, b.RunningIDs) || len(a.Jobs) != len(b.Jobs) {
		return false
	}
	for i := range a.Jobs {
		if a.Jobs[i] != b.Jobs[i] {
			return false
		}
	}
	return reflect.DeepEqual(a.TenantUsed, b.TenantUsed)
}

// ---- 时间回退 ----

func TestRehearsalTimeCannotGoBack(t *testing.T) {
	t0 := time.Date(2026, 9, 30, 14, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	sp := filepath.Join(dir, "s.json")
	seedStateFile(t, sp, t0)
	rh, err := NewRehearsalFromStateFile(sp, filepath.Join(dir, "r.json"), testConfig(nil), t0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rh.Close() })
	if _, err := rh.Apply(NewAdvanceEvent(t0.Add(10 * time.Second))); err != nil {
		t.Fatal(err)
	}
	before := rh.Applied()
	_, err = rh.Apply(NewAdvanceEvent(t0.Add(9 * time.Second)))
	if !errors.Is(err, ErrRehearsalTimeGoesBack) {
		t.Fatalf("want ErrRehearsalTimeGoesBack, got %v", err)
	}
	if rh.Applied() != before {
		t.Fatalf("backwards advance consumed a step")
	}
	// 重启后同样拒绝。
	r2, err := OpenRehearsal(filepath.Join(dir, "r.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r2.Apply(NewAdvanceEvent(t0.Add(8 * time.Second))); !errors.Is(err, ErrRehearsalTimeGoesBack) {
		t.Fatalf("after reopen: want time-goes-back, got %v", err)
	}
	if !r2.Now().Equal(t0.Add(10 * time.Second)) {
		t.Fatalf("logical time altered by rejected advance: %v", r2.Now())
	}
}

// ---- 存档入口校验 ----

func TestRehearsalCheckpointGuards(t *testing.T) {
	t0 := time.Date(2026, 9, 30, 15, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	sp := filepath.Join(dir, "s.json")
	cp := filepath.Join(dir, "r.json")
	seedStateFile(t, sp, t0)

	rh, err := NewRehearsalFromStateFile(sp, cp, testConfig(nil), t0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rh.Apply(testEvents(t0)[0]); err != nil {
		t.Fatal(err)
	}
	_ = rh.Close()

	// 同一路径再新建必须拒绝，防止覆盖进度。
	if _, err := NewRehearsalFromStateFile(sp, cp, testConfig(nil), t0); !errors.Is(err, ErrRehearsalCheckpointExists) {
		t.Fatalf("want ErrRehearsalCheckpointExists, got %v", err)
	}
	// Open 一个普通状态文件（非推演存档）必须明确报错。
	if _, err := OpenRehearsal(sp); !errors.Is(err, ErrRehearsalNotACheckpoint) {
		t.Fatalf("want ErrRehearsalNotACheckpoint, got %v", err)
	}
	// Open 不存在的文件。
	if _, err := OpenRehearsal(filepath.Join(dir, "missing.json")); !errors.Is(err, ErrRehearsalNotACheckpoint) {
		t.Fatalf("missing file: want ErrRehearsalNotACheckpoint, got %v", err)
	}
}

// ---- 与已有持久化数据的兼容性 ----

// TestRehearsalCompatibilityWithSchedulerStore 普通调度器状态文件可直接作为
// 推演种子；推演存档的状态字段与普通存档同构（信封字段被旧加载逻辑忽略）。
func TestRehearsalCompatibilityWithSchedulerStore(t *testing.T) {
	t0 := time.Date(2026, 9, 30, 16, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	sp := filepath.Join(dir, "state.json")
	cp := filepath.Join(dir, "rehearsal.json")

	// 1) 真实调度器写出的状态文件，直接作为推演种子，作业/队列/账目全部保留。
	seedStateFile(t, sp, t0)
	rh, err := NewRehearsalFromStateFile(sp, cp, testConfig(nil), t0)
	if err != nil {
		t.Fatalf("seed from scheduler state: %v", err)
	}
	snap := rh.Snapshot()
	// 两个种子作业各需 2 CPU，容量 6 内均直接启动。
	if !equalStrings(snap.RunningIDs, []string{"seed-queue", "seed-run"}) ||
		snap.Used != (Resource{CPU: 4}) {
		t.Fatalf("seed state not preserved: %s", dumpSnap(snap))
	}
	if _, err := rh.Apply(testEvents(t0)[0]); err != nil {
		t.Fatal(err)
	}
	_ = rh.Close()

	// 2) 推演存档包含普通状态文件的全部键：旧版 Scheduler 的加载器可将其作为
	//    普通状态读出（信封/进度字段被忽略），运行/排队作业与序号一致。
	store, err := newFileStore(cp)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("scheduler loader read rehearsal checkpoint: %v", err)
	}
	if st.NextSeq == 0 || st.Jobs["seed-run"] == nil || st.Jobs["seed-run"].State != StateRunning {
		t.Fatalf("checkpoint not readable as scheduler state: %+v", st)
	}
	if _, ok := st.Jobs["a1"]; !ok {
		t.Fatalf("checkpoint missing rehearsed job a1")
	}

	// 3) 用这份“被普通调度器读过”的状态再恢复一台真实调度器：可以正常工作，
	//    证明数据格式向后兼容（不会因为多出信封字段而损坏）。
	clk := newFakeClock()
	clk.t = rh.Now()
	s2 := newPersistentScheduler(t, cp, clk, Resource{CPU: 6}, testTenants())
	t.Cleanup(func() { _ = s2.Close() })
	mustState(t, s2, "seed-run", StateRunning)
}

// TestRehearsalRejectedStepsSurviveRestart 含业务错误的步骤（拒绝/状态错误）
// 同样随存档保存，重启后历史错误与不中断时逐项一致。
func TestRehearsalRejectedStepsSurviveRestart(t *testing.T) {
	t0 := time.Date(2026, 9, 30, 17, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	cp := filepath.Join(dir, "r.json")
	seed := filepath.Join(t.TempDir(), "s.json")
	seedStateFile(t, seed, t0)

	rh, err := NewRehearsalFromStateFile(seed, cp, testConfig(nil), t0)
	if err != nil {
		t.Fatal(err)
	}
	reps, err := rh.ApplyAll(testEvents(t0))
	if err != nil {
		t.Fatal(err)
	}
	_ = rh.Close()

	r2, err := OpenRehearsal(cp)
	if err != nil {
		t.Fatal(err)
	}
	got := r2.Steps()
	if len(got) != len(reps) {
		t.Fatalf("steps after restart = %d, want %d", len(got), len(reps))
	}
	for i := range reps {
		if !reportsEqualForHistory(reps[i], got[i]) {
			t.Fatalf("step %d mismatch after restart:\n base err=%q\n got  err=%q",
				i, fmtReject(reps[i].Err), fmtReject(got[i].Err))
		}
	}
	// 显式抽查几类错误确实被还原成正确类型。
	errKinds := map[int]string{}
	for i, s := range got {
		if s.Err != nil {
			errKinds[i] = fmtReject(s.Err)
		}
	}
	// 与基准同样位置出现同样错误。
	for i, s := range reps {
		if s.Err != nil && errKinds[i] != fmtReject(s.Err) {
			t.Fatalf("err at %d: %q != %q", i, errKinds[i], fmtReject(s.Err))
		}
	}
}
