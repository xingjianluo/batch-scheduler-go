package scheduler

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// 本文件覆盖三类业务场景（运行 go test -v 可在日志中看到各场景用例与结果）：
//  1. 完成/取消一个作业引发的级联调度动作（启动、抢占、重新排队）被逐步完整记录；
//  2. 存档失败后原样重试同一步只生效一次（不多占资源、队列/账目不错位）；
//  3. 与同样顺序真跑一遍的结论逐项一致。

// logStep 以可读形式打印一步推演的完整记录（含连锁裁决）。
func logStep(t *testing.T, rep *RehearsalStepReport) {
	t.Helper()
	t.Logf("step %d: kind=%d job=%q time=%s err=%s snapshotUsed=%+v queued=%v running=%v",
		rep.Index, rep.Event.Kind, rep.Event.JobID, rep.Time.Format(time.RFC3339),
		fmtReject(rep.Err), rep.Snapshot.Used, rep.Snapshot.QueuedIDs, rep.Snapshot.RunningIDs)
	for _, d := range rep.Decisions {
		t.Logf("    action=%-12s job=%-8s state=%-9s used=%+v victims=%v reason=%q",
			d.Action, d.JobID, d.JobState, d.Used, d.VictimIDs, d.Reason)
	}
}

// mustApply 应用事件并要求无基础设施错误。
func mustApply(t *testing.T, r *Rehearsal, ev RehearsalEvent) *RehearsalStepReport {
	t.Helper()
	rep, err := r.Apply(ev)
	if err != nil {
		t.Fatalf("apply %+v: %v", ev, err)
	}
	return rep
}

// actionsOf 提取裁决动作序列，便于断言。
func actionsOf(ds []Decision) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.Action + ":" + d.JobID
	}
	return out
}

// ---- 场景 1a：完成一个运行中作业，连锁启动多个排队作业 ----

func TestRehearsalCompleteCascadeRecorded(t *testing.T) {
	t0 := time.Date(2026, 10, 4, 9, 0, 0, 0, time.UTC)
	t.Log("场景：容量 4 CPU，j1/j2 各占 2CPU 运行，j3/j4 各 1CPU 排队；" +
		"完成 j1 释放 2CPU，应连锁启动 j3 与 j4，且该步完整记录 complete+start+start")

	setup := func(clk *fakeClock) *Scheduler {
		s, err := New(Config{Capacity: Resource{CPU: 4}, Clock: clk},
			[]Tenant{{ID: "a", Quota: Resource{CPU: 4}}})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		mustSubmit(t, s, "j1", "a", 5, Resource{CPU: 2})
		mustSubmit(t, s, "j2", "a", 5, Resource{CPU: 2})
		mustSubmit(t, s, "j3", "a", 5, Resource{CPU: 1})
		mustSubmit(t, s, "j4", "a", 5, Resource{CPU: 1})
		return s
	}

	clkLive := newFakeClock()
	clkLive.t = t0
	live := setup(clkLive)
	t.Cleanup(func() { _ = live.Close() })

	rh, err := NewRehearsalFromLive(live, "")
	if err != nil {
		t.Fatalf("fork: %v", err)
	}
	t.Cleanup(func() { _ = rh.Close() })

	rep := mustApply(t, rh, NewCompleteEvent("j1"))
	logStep(t, rep)

	// 该步必须完整记录：complete(j1) -> start(j3) -> start(j4)。
	want := []string{"complete:j1", "start:j3", "start:j4"}
	if got := actionsOf(rep.Decisions); !equalStrings(got, want) {
		t.Fatalf("cascade actions = %v, want %v", got, want)
	}
	for _, d := range rep.Decisions[1:] {
		if len(d.VictimIDs) != 0 {
			t.Fatalf("unexpected victims in pure-start cascade: %+v", d)
		}
	}

	// 与同样顺序真跑一遍逐项一致。
	clkCtrl := newFakeClock()
	clkCtrl.t = t0
	control := setup(clkCtrl)
	t.Cleanup(func() { _ = control.Close() })
	realDs, err := control.complete("j1")
	if err != nil {
		t.Fatalf("real complete: %v", err)
	}
	if !decisionsEqual(realDs, rep.Decisions) {
		t.Fatalf("cascade mismatch:\n real=%s\n rehe=%s",
			dumpDecisions(realDs), dumpDecisions(rep.Decisions))
	}
	assertSnapshotsDeep(t, control.Snapshot(), rh.Snapshot(), "complete cascade final")
	t.Logf("结果：完成 j1 的级联启动被完整记录且与真跑逐项一致：%s", dumpDecisions(rep.Decisions))
}

// ---- 场景 1b：取消一个运行中作业，连锁触发抢占（谁被抢、谁顶上来）----

func TestRehearsalCancelPreemptionCascadeRecorded(t *testing.T) {
	t0 := time.Date(2026, 10, 4, 10, 0, 0, 0, time.UTC)
	t.Log("场景：容量 4 CPU，keep(1CPU,pri9)/victim(1CPU,pri5)/goner(2CPU,pri9) 运行，" +
		"waiter(3CPU,pri9) 因资源不足排队（goner 同优先级不可被抢）；取消 goner 释放 2CPU 后仍不够 3CPU，" +
		"调度应抢占 victim 让 waiter 顶上来，victim 重新排队；该步须完整记录 cancel+preempt+start")

	setup := func(clk *fakeClock) *Scheduler {
		s, err := New(Config{Capacity: Resource{CPU: 4}, Clock: clk},
			[]Tenant{{ID: "a", Quota: Resource{CPU: 4}}})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		mustSubmit(t, s, "keep", "a", 9, Resource{CPU: 1})
		mustSubmit(t, s, "victim", "a", 5, Resource{CPU: 1})
		mustSubmit(t, s, "goner", "a", 9, Resource{CPU: 2})
		mustSubmit(t, s, "waiter", "a", 9, Resource{CPU: 3})
		return s
	}

	clkLive := newFakeClock()
	clkLive.t = t0
	live := setup(clkLive)
	t.Cleanup(func() { _ = live.Close() })
	// 前置条件：waiter 提交时唯一可抢的 victim 只有 1CPU，凑不齐 3CPU，
	// 因此提交时未发生抢占、waiter 在排队。
	mustState(t, live, "waiter", StateQueued)

	rh, err := NewRehearsalFromLive(live, "")
	if err != nil {
		t.Fatalf("fork: %v", err)
	}
	t.Cleanup(func() { _ = rh.Close() })

	rep := mustApply(t, rh, NewCancelEvent("goner"))
	logStep(t, rep)

	// 该步必须完整记录：cancel(goner) -> preempt(victim) -> start(waiter, victims=[victim])。
	want := []string{"cancel:goner", "preempt:victim", "start:waiter"}
	if got := actionsOf(rep.Decisions); !equalStrings(got, want) {
		t.Fatalf("cascade actions = %v, want %v", got, want)
	}
	preempt := rep.Decisions[1]
	if preempt.JobState != StateQueued {
		t.Fatalf("preempted victim must be recorded as re-queued, got %s", preempt.JobState)
	}
	start := rep.Decisions[2]
	if !equalStrings(start.VictimIDs, []string{"victim"}) {
		t.Fatalf("start victims = %v, want [victim]", start.VictimIDs)
	}
	// 步内快照：victim 重新排队、waiter 顶上运行。
	mustSnapState(t, rep.Snapshot, "victim", StateQueued)
	mustSnapState(t, rep.Snapshot, "waiter", StateRunning)

	// 与同样顺序真跑一遍逐项一致。
	clkCtrl := newFakeClock()
	clkCtrl.t = t0
	control := setup(clkCtrl)
	t.Cleanup(func() { _ = control.Close() })
	realDs, err := control.cancel("goner")
	if err != nil {
		t.Fatalf("real cancel: %v", err)
	}
	if !decisionsEqual(realDs, rep.Decisions) {
		t.Fatalf("cascade mismatch:\n real=%s\n rehe=%s",
			dumpDecisions(realDs), dumpDecisions(rep.Decisions))
	}
	assertSnapshotsDeep(t, control.Snapshot(), rh.Snapshot(), "cancel preempt cascade final")
	t.Logf("结果：取消 goner 引发的抢占级联被完整记录且与真跑逐项一致：%s",
		dumpDecisions(rep.Decisions))
}

// ---- 场景 1c：完成一个作业同样能引发抢占级联（对称路径）----

func TestRehearsalCompletePreemptionCascadeRecorded(t *testing.T) {
	t0 := time.Date(2026, 10, 4, 11, 0, 0, 0, time.UTC)
	t.Log("场景：同 1b，但把触发动作换成“完成 goner”：释放的 2CPU 不够 waiter(3CPU)，" +
		"须抢占 victim；该步须完整记录 complete+preempt+start")

	setup := func(clk *fakeClock) *Scheduler {
		s, err := New(Config{Capacity: Resource{CPU: 4}, Clock: clk},
			[]Tenant{{ID: "a", Quota: Resource{CPU: 4}}})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		mustSubmit(t, s, "keep", "a", 9, Resource{CPU: 1})
		mustSubmit(t, s, "victim", "a", 5, Resource{CPU: 1})
		mustSubmit(t, s, "goner", "a", 9, Resource{CPU: 2})
		mustSubmit(t, s, "waiter", "a", 9, Resource{CPU: 3})
		return s
	}

	clkLive := newFakeClock()
	clkLive.t = t0
	live := setup(clkLive)
	t.Cleanup(func() { _ = live.Close() })

	rh, err := NewRehearsalFromLive(live, "")
	if err != nil {
		t.Fatalf("fork: %v", err)
	}
	t.Cleanup(func() { _ = rh.Close() })

	rep := mustApply(t, rh, NewCompleteEvent("goner"))
	logStep(t, rep)

	want := []string{"complete:goner", "preempt:victim", "start:waiter"}
	if got := actionsOf(rep.Decisions); !equalStrings(got, want) {
		t.Fatalf("cascade actions = %v, want %v", got, want)
	}
	if !equalStrings(rep.Decisions[2].VictimIDs, []string{"victim"}) {
		t.Fatalf("start victims = %v, want [victim]", rep.Decisions[2].VictimIDs)
	}

	clkCtrl := newFakeClock()
	clkCtrl.t = t0
	control := setup(clkCtrl)
	t.Cleanup(func() { _ = control.Close() })
	realDs, err := control.complete("goner")
	if err != nil {
		t.Fatalf("real complete: %v", err)
	}
	if !decisionsEqual(realDs, rep.Decisions) {
		t.Fatalf("cascade mismatch:\n real=%s\n rehe=%s",
			dumpDecisions(realDs), dumpDecisions(rep.Decisions))
	}
	t.Logf("结果：完成 goner 引发的抢占级联被完整记录且与真跑逐项一致：%s",
		dumpDecisions(rep.Decisions))
}

// mustSnapState 断言快照中某作业的状态。
func mustSnapState(t *testing.T, snap *Snapshot, id string, want JobState) {
	t.Helper()
	for _, j := range snap.Jobs {
		if j.ID == id {
			if j.State != want {
				t.Fatalf("snapshot job %s state = %s, want %s", id, j.State, want)
			}
			return
		}
	}
	t.Fatalf("snapshot missing job %s", id)
}

// ---- 场景 2：存档失败后原样重试同一步只生效一次 ----

func TestRehearsalSaveFailureRetryExactlyOnce(t *testing.T) {
	t0 := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	t.Log("场景：提交 retry-job(2CPU) 时存档注入失败；该步对外与重启后都不可见；" +
		"原样重试只生效一次（资源只计一次），再次重复提交只得到 DuplicateJob 拒绝且不重复占资源")

	dir := t.TempDir()
	cpPath := filepath.Join(dir, "rehearsal.json")
	fail := false
	r := failingSaveRehearsal(t, cpPath, &fail, t0)

	ev := NewSubmitEvent("retry-job", "a", 1, Resource{CPU: 2})
	before := r.Snapshot()
	beforeUsed := before.Used
	t.Logf("失败前：used=%+v queued=%v running=%v", beforeUsed, before.QueuedIDs, before.RunningIDs)

	// 第一次尝试：存档失败。
	fail = true
	rep, err := r.Apply(ev)
	if !errors.Is(err, errInjectedSave) {
		t.Fatalf("want injected save error, got rep=%v err=%v", rep, err)
	}
	if rep != nil {
		t.Fatalf("failed step must not return a report")
	}
	// 对外不可见：内存状态原地不动。
	after := r.Snapshot()
	assertResource(t, after.Used, beforeUsed, "used after failed save")
	if containsJob(after.Jobs, "retry-job") {
		t.Fatalf("failed step leaked job into memory state")
	}

	// 重启后也不可见：磁盘存档里没有这一步。
	r2, err := OpenRehearsal(cpPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if containsJob(r2.Snapshot().Jobs, "retry-job") {
		t.Fatalf("failed step leaked into checkpoint")
	}
	if r2.Applied() != r.Applied() {
		t.Fatalf("applied mismatch after reopen: mem=%d disk=%d", r.Applied(), r2.Applied())
	}
	t.Logf("存档失败步在内存与磁盘中均不可见：applied=%d used=%+v", r2.Applied(), r2.Snapshot().Used)

	// 原样重试同一步：成功且只生效一次。
	fail = false
	rep2, err := r2.Apply(ev)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	logStep(t, rep2)
	if rep2.Submit == nil || !rep2.Submit.Started {
		t.Fatalf("retried submit should be accepted and started: %+v", rep2.Submit)
	}
	wantUsed := beforeUsed.Add(Resource{CPU: 2})
	assertResource(t, r2.Snapshot().Used, wantUsed, "used after exactly-once retry")

	// 再次重复同一提交：只得到 DuplicateJob 业务拒绝，资源不再增加。
	rep3, err := r2.Apply(ev)
	if err != nil {
		t.Fatalf("duplicate apply: %v", err)
	}
	var rj *RejectError
	if !errors.As(rep3.Err, &rj) || rj.Kind != RejectDuplicateJob {
		t.Fatalf("second identical submit: want DuplicateJob reject, got %s", fmtReject(rep3.Err))
	}
	assertResource(t, r2.Snapshot().Used, wantUsed, "used after duplicate submit")
	t.Logf("重复提交被拒（%s），资源账目保持 %+v，未重复占用", fmtReject(rep3.Err), r2.Snapshot().Used)

	// 续跑语义保持：再次重启后，该步在历史中只出现一次，快照一致。
	r3, err := OpenRehearsal(cpPath)
	if err != nil {
		t.Fatalf("second reopen: %v", err)
	}
	cnt := 0
	for _, s := range r3.Steps() {
		if s.Event.Kind == RehearsalSubmit && s.Event.JobID == "retry-job" && s.Submit != nil {
			cnt++
		}
	}
	if cnt != 1 {
		t.Fatalf("accepted retry-job step appears %d times in history, want exactly 1", cnt)
	}
	assertResource(t, r3.Snapshot().Used, wantUsed, "used after second reopen")
	assertInvariants(t, r3.Snapshot(), tenantMap(testTenants()), "exactly-once")
	t.Logf("结果：重试只生效一次，重启后历史中受理记录恰出现 1 次，资源账目 %+v 一致",
		r3.Snapshot().Used)
}
