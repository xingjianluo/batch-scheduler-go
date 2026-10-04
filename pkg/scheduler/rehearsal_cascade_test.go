package scheduler

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// 本文件覆盖推演（Rehearsal）的三类业务场景：
//  1. 完成/取消引发的级联调度动作（启动、抢占、重新入队）被逐步完整记录，
//     并与同样顺序在真实调度器上跑一遍得到的结论逐项一致；
//  2. 在多个不同中断点“杀掉进程”续跑后，每一步历史记录（当时状态快照、
//     提交受理/拒绝、失败原因、全部裁决）与一口气跑完逐项一致；
//  3. 某一步存档失败时该步及其全部连锁对外与重启后均不可见，
//     原样重试只生效一次、不多占资源、队列与资源账目不错位。

// cascadeConfig 级联场景专用配置：集群 4 CPU，便于构造“完成只释放部分
// 资源、排队作业仍需抢占”的情形。
func cascadeConfig() Config {
	cfg := testConfig(nil)
	cfg.Capacity = Resource{CPU: 4}
	return cfg
}

// cascadeTenants 级联场景专用租户：租户 a 限制并发 1，使得“高优先级作业
// 提交时因并发槽被同租户作业占用而排队、等该作业完成后才触发抢占”这一
// 级联路径可以被确定性地构造出来。
func cascadeTenants() []Tenant {
	return []Tenant{
		{ID: "a", Quota: Resource{CPU: 4}, MaxConcurrent: 1},
		{ID: "b", Quota: Resource{CPU: 4}},
	}
}

// applyEventToReal 在真实调度器上应用一个推演事件，返回完整裁决序列
// （Complete/Cancel 走 CompleteAll/CancelAll，拿到全部连锁裁决）。
func applyEventToReal(t *testing.T, s *Scheduler, clk *fakeClock, ev RehearsalEvent) ([]Decision, *SubmitResult, error) {
	t.Helper()
	switch ev.Kind {
	case RehearsalSubmit:
		res, err := s.Submit(ev.JobID, ev.TenantID, ev.Priority, ev.Request)
		if res != nil {
			return res.Decisions, res, err
		}
		return nil, nil, err
	case RehearsalComplete:
		ds, err := s.CompleteAll(ev.JobID)
		return ds, nil, err
	case RehearsalCancel:
		ds, err := s.CancelAll(ev.JobID)
		return ds, nil, err
	case RehearsalAdvance:
		clk.t = ev.At
		ds, err := s.Tick(ev.At)
		return ds, nil, err
	}
	return nil, nil, ErrInvalidArgument
}

// logStep 以统一格式输出一步推演的记录（测试日志要求：能看出每步
// 发生了什么调度动作、谁被抢占、谁顶上来）。
func logStep(t *testing.T, prefix string, rep *RehearsalStepReport) {
	t.Helper()
	t.Logf("    step %d kind=%d job=%q err=%s decisions=%s",
		rep.Index, rep.Event.Kind, rep.Event.JobID, fmtReject(rep.Err), dumpDecisions(rep.Decisions))
	if rep.Snapshot != nil {
		t.Logf("      %s snapshot: used=%+v queued=%v running=%v",
			prefix, rep.Snapshot.Used, rep.Snapshot.QueuedIDs, rep.Snapshot.RunningIDs)
	}
}

// TestRehearsalCascadeDecisionsRecorded 完成/取消单个作业时，由它连锁引发
// 的启动/抢占/重新入队必须完整出现在该步记录中，且与真实调度器同序跑一遍
// 的每步结论（裁决 + 状态快照）逐项一致。
func TestRehearsalCascadeDecisionsRecorded(t *testing.T) {
	t0 := time.Date(2026, 10, 4, 9, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		desc   string
		events []RehearsalEvent
	}{
		{
			name: "complete-triggers-cascade-start",
			desc: "集群满时有作业排队；完成一个运行作业释放资源，排队作业随之启动",
			events: []RehearsalEvent{
				NewSubmitEvent("a1", "a", 1, Resource{CPU: 2}),
				NewSubmitEvent("b1", "b", 1, Resource{CPU: 2}),
				NewSubmitEvent("c1", "a", 1, Resource{CPU: 2}), // 集群满，排队
				NewCompleteEvent("a1"),                         // 连锁：c1 启动
			},
		},
		{
			name: "complete-triggers-preemption",
			desc: "完成只释放部分资源，排队的高优先级作业仍放不下，转而抢占低优先级运行作业",
			events: []RehearsalEvent{
				NewSubmitEvent("ax", "a", 9, Resource{CPU: 1}),
				NewSubmitEvent("blow", "b", 1, Resource{CPU: 3}),
				NewSubmitEvent("ahigh", "a", 5, Resource{CPU: 3}), // 排队
				NewCompleteEvent("ax"),                            // 连锁：抢占 blow、启动 ahigh
			},
		},
		{
			name: "cancel-running-triggers-cascade-start",
			desc: "取消一个运行作业释放资源，排队作业随之启动",
			events: []RehearsalEvent{
				NewSubmitEvent("d1", "a", 1, Resource{CPU: 2}),
				NewSubmitEvent("e1", "b", 1, Resource{CPU: 2}),
				NewSubmitEvent("f1", "a", 1, Resource{CPU: 2}), // 排队
				NewCancelEvent("d1"),                           // 连锁：f1 启动
			},
		},
		{
			name: "cancel-queued-has-no-cascade",
			desc: "取消的是排队作业本身，不释放任何资源，不应产生连锁动作",
			events: []RehearsalEvent{
				NewSubmitEvent("g1", "a", 1, Resource{CPU: 3}),
				NewSubmitEvent("h1", "b", 1, Resource{CPU: 2}), // 排队
				NewCancelEvent("h1"),                           // 仅取消本身
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("场景: %s", tc.desc)
			clk := newFakeClock()
			clk.t = t0
			real, err := New(Config{
				Capacity:     Resource{CPU: 4},
				AgeIncrement: 10,
				AgingPeriod:  10 * time.Second,
				Clock:        clk,
			}, cascadeTenants())
			if err != nil {
				t.Fatalf("new real: %v", err)
			}
			t.Cleanup(func() { _ = real.Close() })

			rh, err := NewRehearsalFromLive(real, "")
			if err != nil {
				t.Fatalf("new rehearsal: %v", err)
			}

			for i, ev := range tc.events {
				rep, err := rh.Apply(ev)
				if err != nil {
					t.Fatalf("rehearsal apply %d: %v", i, err)
				}
				realDs, realSub, realErr := applyEventToReal(t, real, clk, ev)
				logStep(t, "rehearsal", rep)
				t.Logf("      real     decisions=%s", dumpDecisions(realDs))

				if !errorsEqual(realErr, rep.Err) {
					t.Fatalf("step %d err mismatch: real=%q rehearsal=%q",
						i, fmtReject(realErr), fmtReject(rep.Err))
				}
				if !decisionsEqual(realDs, rep.Decisions) {
					t.Fatalf("step %d decisions mismatch:\n real=%s\n rehearsal=%s",
						i, dumpDecisions(realDs), dumpDecisions(rep.Decisions))
				}
				if (realSub != nil) != (rep.Submit != nil) {
					t.Fatalf("step %d submit-accepted mismatch: real=%v rehearsal=%v",
						i, realSub != nil, rep.Submit != nil)
				}
				if realSub != nil && realSub.Started != rep.Submit.Started {
					t.Fatalf("step %d submit started mismatch", i)
				}
				// 每步记录的状态快照必须与真跑后的真实状态逐项一致。
				if !snapshotsStrictEqual(real.Snapshot(), rep.Snapshot) {
					t.Fatalf("step %d snapshot mismatch:\n real=%+v\n rehearsal=%+v",
						i, real.Snapshot(), rep.Snapshot)
				}
			}
			t.Logf("结果: %d 步全部与真跑逐项一致（裁决 + 状态快照）", len(tc.events))
		})
	}
}

// TestRehearsalCompletePreemptionCascadeDetail 逐项检查“完成引发抢占”这一步
// 的记录内容：谁被抢占、谁顶上来、被抢占者按原序号重新入队、资源账目逐步正确。
func TestRehearsalCompletePreemptionCascadeDetail(t *testing.T) {
	t0 := time.Date(2026, 10, 4, 10, 0, 0, 0, time.UTC)
	t.Logf("场景: 集群 4CPU，ax(1CPU,pri9) 与 blow(3CPU,pri1) 在跑，ahigh(3CPU,pri5) 排队；" +
		"完成 ax 只释放 1CPU，ahigh 仍放不下 -> 抢占 blow 顶上")

	clk := newFakeClock()
	clk.t = t0
	real, err := New(Config{
		Capacity:     Resource{CPU: 4},
		AgeIncrement: 10,
		AgingPeriod:  10 * time.Second,
		Clock:        clk,
	}, cascadeTenants())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = real.Close() })
	rh, err := NewRehearsalFromLive(real, "")
	if err != nil {
		t.Fatal(err)
	}
	setup := []RehearsalEvent{
		NewSubmitEvent("ax", "a", 9, Resource{CPU: 1}),
		NewSubmitEvent("blow", "b", 1, Resource{CPU: 3}),
		NewSubmitEvent("ahigh", "a", 5, Resource{CPU: 3}),
	}
	for _, ev := range setup {
		if _, err := rh.Apply(ev); err != nil {
			t.Fatal(err)
		}
		if _, _, err := applyEventToReal(t, real, clk, ev); err != nil {
			t.Fatal(err)
		}
	}

	rep, err := rh.Apply(NewCompleteEvent("ax"))
	if err != nil {
		t.Fatal(err)
	}
	logStep(t, "rehearsal", rep)

	// 该步必须完整记录三个动作：完成、抢占、启动。
	if len(rep.Decisions) != 3 {
		t.Fatalf("complete step decisions = %s, want 3 entries", dumpDecisions(rep.Decisions))
	}
	if rep.Decisions[0].Action != "complete" || rep.Decisions[0].JobID != "ax" {
		t.Fatalf("decisions[0] = %+v, want complete ax", rep.Decisions[0])
	}
	if rep.Decisions[1].Action != "preempt" || rep.Decisions[1].JobID != "blow" {
		t.Fatalf("decisions[1] = %+v, want preempt blow", rep.Decisions[1])
	}
	st := rep.Decisions[2]
	if st.Action != "start" || st.JobID != "ahigh" || !equalStrings(st.VictimIDs, []string{"blow"}) {
		t.Fatalf("decisions[2] = %+v, want start ahigh victims=[blow]", st)
	}
	// 资源账目逐步可见：完成释放 1、抢占再释放 3、启动占用 3。
	assertResource(t, rep.Decisions[0].Used, Resource{CPU: 3}, "used after complete")
	assertResource(t, rep.Decisions[1].Used, Resource{CPU: 0}, "used after preempt")
	assertResource(t, rep.Decisions[2].Used, Resource{CPU: 3}, "used after start")

	// 本步快照：ahigh 顶上运行；blow 被抢后按原 SubmitSeq 回到队列、计 1 次被抢。
	snap := rep.Snapshot
	if !equalStrings(snap.RunningIDs, []string{"ahigh"}) {
		t.Fatalf("running = %v, want [ahigh]", snap.RunningIDs)
	}
	if !equalStrings(snap.QueuedIDs, []string{"blow"}) {
		t.Fatalf("queued = %v, want [blow]", snap.QueuedIDs)
	}
	var blowJob *Job
	for i := range snap.Jobs {
		if snap.Jobs[i].ID == "blow" {
			blowJob = &snap.Jobs[i]
		}
	}
	if blowJob == nil || blowJob.State != StateQueued || blowJob.PreemptedTimes != 1 || blowJob.SubmitSeq != 2 {
		t.Fatalf("blow after preempt = %+v, want queued/preempted=1/seq=2", blowJob)
	}

	// 与真跑同序结论逐项一致。
	realDs, _, realErr := applyEventToReal(t, real, clk, NewCompleteEvent("ax"))
	if realErr != nil || !decisionsEqual(realDs, rep.Decisions) ||
		!snapshotsStrictEqual(real.Snapshot(), rep.Snapshot) {
		t.Fatalf("real run mismatch:\n real=%s\n rehearsal=%s",
			dumpDecisions(realDs), dumpDecisions(rep.Decisions))
	}

	// 后续：取消 ahigh，blow 应随即顶上（取消引发的级联启动）。
	rep2, err := rh.Apply(NewCancelEvent("ahigh"))
	if err != nil {
		t.Fatal(err)
	}
	logStep(t, "rehearsal", rep2)
	if len(rep2.Decisions) != 2 || rep2.Decisions[1].Action != "start" || rep2.Decisions[1].JobID != "blow" {
		t.Fatalf("cancel cascade = %s, want [cancel ahigh, start blow]",
			dumpDecisions(rep2.Decisions))
	}
	t.Logf("结果: 抢占者/被抢者/重新入队/资源账目均完整记录且与真跑一致")
}

// cascadeEvents 级联密集的事件序列：覆盖完成引发抢占、取消引发启动、
// 提交受理并直接启动、终态 ID 复用被拒、空级联完成、作业不存在错误、
// 纯时间推进，用于“中断续跑 vs 一口气跑完”的逐步对照。
func cascadeEvents(t0 time.Time) []RehearsalEvent {
	return []RehearsalEvent{
		NewSubmitEvent("ax", "a", 9, Resource{CPU: 1}),
		NewSubmitEvent("blow", "b", 1, Resource{CPU: 3}),
		NewSubmitEvent("ahigh", "a", 5, Resource{CPU: 3}), // 排队
		NewCompleteEvent("ax"),                            // 连锁：抢占 blow、启动 ahigh
		NewAdvanceEvent(t0.Add(5 * time.Second)),
		NewCancelEvent("ahigh"), // 连锁：启动 blow
		NewSubmitEvent("a2", "a", 1, Resource{CPU: 1}),  // 受理并直接启动
		NewSubmitEvent("ax", "a", 9, Resource{CPU: 1}),  // 终态 ID 复用 -> DuplicateJob 拒绝
		NewCompleteEvent("blow"),                        // 无排队作业，仅完成本身
		NewCompleteEvent("ghost"),                       // ErrJobNotFound
		NewAdvanceEvent(t0.Add(30 * time.Second)),
	}
}

// seedCascadeTenantState 生成只含租户、不含作业的状态文件（推演从空集群开始）。
func seedCascadeTenantState(t *testing.T, path string, t0 time.Time) {
	t.Helper()
	clk := newFakeClock()
	clk.t = t0
	s := newPersistentScheduler(t, path, clk, Resource{CPU: 4}, cascadeTenants())
	if err := s.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}
}

// TestRehearsalCascadeResumeIdentical 在级联密集的事件序列上，于每一个可能的
// 中断点“杀掉进程”（不 Close、直接丢弃）再 OpenRehearsal 续跑：
// 恢复出的每一步历史记录（当时状态快照、提交受理/拒绝、失败原因、全部
// 调度裁决）与一口气跑完逐项一致，最终状态也一致。
func TestRehearsalCascadeResumeIdentical(t *testing.T) {
	t0 := time.Date(2026, 10, 4, 11, 0, 0, 0, time.UTC)
	events := cascadeEvents(t0)
	t.Logf("场景: %d 步级联密集事件（完成引发抢占、取消引发启动、受理/拒绝、错误步），"+
		"在 %d 个不同中断点杀掉续跑", len(events), len(events)+1)

	// 基准：一口气跑完。
	baseDir := t.TempDir()
	baseSeed := filepath.Join(baseDir, "state.json")
	seedCascadeTenantState(t, baseSeed, t0)
	baseRh, err := NewRehearsalFromStateFile(baseSeed, filepath.Join(baseDir, "r.json"), cascadeConfig(), t0)
	if err != nil {
		t.Fatal(err)
	}
	baseReps, err := baseRh.ApplyAll(events)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = baseRh.Close() })

	for cutoff := 0; cutoff <= len(events); cutoff++ {
		cutoff := cutoff
		t.Run("cutoff-"+itoa(cutoff), func(t *testing.T) {
			dir := t.TempDir()
			seed := filepath.Join(dir, "state.json")
			cp := filepath.Join(dir, "r.json")
			seedCascadeTenantState(t, seed, t0)

			rh, err := NewRehearsalFromStateFile(seed, cp, cascadeConfig(), t0)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < cutoff; i++ {
				if _, err := rh.Apply(events[i]); err != nil {
					t.Fatalf("apply %d: %v", i, err)
				}
			}
			rh = nil // 模拟进程被杀：不 Close，仅靠每步原子存档

			cont, err := OpenRehearsal(cp)
			if err != nil {
				t.Fatalf("open at cutoff %d: %v", cutoff, err)
			}
			if cont.Applied() != cutoff {
				t.Fatalf("applied after reopen = %d, want %d", cont.Applied(), cutoff)
			}
			for i := cutoff; i < len(events); i++ {
				if _, err := cont.Apply(events[i]); err != nil {
					t.Fatalf("resume apply %d: %v", i, err)
				}
			}

			// 每一步历史记录逐项一致（含当时快照、受理/拒绝、失败原因、全部裁决）。
			got := cont.Steps()
			if len(got) != len(baseReps) {
				t.Fatalf("steps = %d, want %d", len(got), len(baseReps))
			}
			for i := range baseReps {
				if !reportsEqualForHistory(baseReps[i], got[i]) {
					t.Fatalf("step %d differs after resume at cutoff %d:\n base=%s\n got =%s",
						i, cutoff, dumpStep(baseReps[i]), dumpStep(got[i]))
				}
			}
			// 最终状态逐项一致。
			if !snapshotsStrictEqual(baseRh.Snapshot(), cont.Snapshot()) {
				t.Fatalf("final snapshot differs after resume at cutoff %d", cutoff)
			}
			t.Logf("结果: 中断点 %d 续跑，%d 步历史记录与最终状态逐项一致", cutoff, len(got))
			_ = cont.Close()
		})
	}
}

// TestRehearsalCascadeSaveFailureRetryOnce 某一步（完成引发抢占级联）存档失败时：
// 该步及其全部连锁对外与重启后均不可见；原样重试只生效一次、不多占资源；
// 续跑（杀掉重开）之后同样的失败/重试语义保持不变。
func TestRehearsalCascadeSaveFailureRetryOnce(t *testing.T) {
	t0 := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	t.Logf("场景: 完成 ax 会引发抢占级联；让该步存档失败，验证连锁完全不可见、" +
		"重试只生效一次；杀掉重开后续跑，同样的失败/重试语义保持")

	// 种子：ax/blow 在跑、ahigh 排队（由真实调度器产生）。
	seed := filepath.Join(t.TempDir(), "state.json")
	clk := newFakeClock()
	clk.t = t0
	s := newPersistentScheduler(t, seed, clk, Resource{CPU: 4}, cascadeTenants())
	mustSubmit(t, s, "ax", "a", 9, Resource{CPU: 1})
	mustSubmit(t, s, "blow", "b", 1, Resource{CPU: 3})
	mustSubmit(t, s, "ahigh", "a", 5, Resource{CPU: 3})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 基准：同样事件一口气跑完（正常存档）。
	baseDir := t.TempDir()
	baseRh, err := NewRehearsalFromStateFile(seed, filepath.Join(baseDir, "r.json"), cascadeConfig(), t0)
	if err != nil {
		t.Fatal(err)
	}
	events := []RehearsalEvent{
		NewCompleteEvent("ax"),    // 连锁：抢占 blow、启动 ahigh
		NewCancelEvent("ahigh"),   // 连锁：启动 blow
		NewCompleteEvent("blow"),  // 仅完成
	}
	baseReps, err := baseRh.ApplyAll(events)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = baseRh.Close() })

	// 可注入存档失败的推演器。
	dir := t.TempDir()
	cp := filepath.Join(dir, "r.json")
	store, err := newFileStore(seed)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	r, err := newRehearsal(cp, cascadeConfig(), t0, st, true)
	if err != nil {
		t.Fatal(err)
	}
	fail := false
	r.saveFunc = func(c *rehearsalCheckpoint) error {
		if fail {
			return errInjectedSave
		}
		return saveRehearsalCheckpoint(cp, c)
	}

	beforeSnap := r.Snapshot()
	// 第 0 步（完成 ax，会引发抢占级联）存档失败。
	fail = true
	rep, err := r.Apply(events[0])
	if !errors.Is(err, errInjectedSave) {
		t.Fatalf("want injected save error, got %v (rep=%v)", err, rep)
	}
	if rep != nil {
		t.Fatalf("failed step must not return a report")
	}
	// 内存中：该步及其连锁（抢占/启动）完全不可见，资源账目原地不动。
	if r.Applied() != 0 {
		t.Fatalf("applied = %d, want 0", r.Applied())
	}
	if !snapshotsStrictEqual(beforeSnap, r.Snapshot()) {
		t.Fatalf("failed step leaked into memory state")
	}
	// 重启后：磁盘上也看不到该步任何痕迹。
	r2, err := OpenRehearsal(cp)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Applied() != 0 || !snapshotsStrictEqual(beforeSnap, r2.Snapshot()) {
		t.Fatalf("failed step leaked into disk state")
	}
	t.Logf("结果: 存档失败步及其抢占/启动连锁在内存与磁盘中均不可见")

	// 原样重试同一步：只生效一次。
	fail = false
	rep2, err := r2.Apply(events[0])
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	logStep(t, "retried", rep2)
	if !reportsEqualForHistory(baseReps[0], *rep2) {
		t.Fatalf("retried step differs from baseline:\n base=%s\n got =%s",
			dumpStep(baseReps[0]), dumpStep(*rep2))
	}
	// 资源账目精确：ahigh 占 3CPU 运行，blow 被抢回队列且只被抢一次。
	snap := r2.Snapshot()
	assertResource(t, snap.Used, Resource{CPU: 3}, "used after retried complete")
	if !equalStrings(snap.RunningIDs, []string{"ahigh"}) || !equalStrings(snap.QueuedIDs, []string{"blow"}) {
		t.Fatalf("after retry running=%v queued=%v", snap.RunningIDs, snap.QueuedIDs)
	}
	for i := range snap.Jobs {
		if snap.Jobs[i].ID == "blow" && snap.Jobs[i].PreemptedTimes != 1 {
			t.Fatalf("blow preempted %d times, want exactly 1 (retry must not double-apply)",
				snap.Jobs[i].PreemptedTimes)
		}
	}

	// 杀掉重开，续跑剩余步骤；期间再让一步（取消 ahigh 的级联）存档失败。
	r2 = nil
	r3, err := OpenRehearsal(cp)
	if err != nil {
		t.Fatal(err)
	}
	if r3.Applied() != 1 {
		t.Fatalf("applied after reopen = %d, want 1", r3.Applied())
	}
	// 历史第 0 步与基准逐项一致（含当时快照）。
	if !reportsEqualForHistory(baseReps[0], r3.Steps()[0]) {
		t.Fatalf("history step 0 differs after reopen")
	}
	r3.saveFunc = func(c *rehearsalCheckpoint) error {
		if fail {
			return errInjectedSave
		}
		return saveRehearsalCheckpoint(cp, c)
	}
	snapBeforeCancel := r3.Snapshot()
	fail = true
	if _, err := r3.Apply(events[1]); !errors.Is(err, errInjectedSave) {
		t.Fatalf("want injected save error on cancel cascade, got %v", err)
	}
	if !snapshotsStrictEqual(snapBeforeCancel, r3.Snapshot()) || r3.Applied() != 1 {
		t.Fatalf("failed cancel cascade leaked after resume")
	}
	fail = false
	if _, err := r3.Apply(events[1]); err != nil {
		t.Fatalf("retry cancel: %v", err)
	}
	if _, err := r3.Apply(events[2]); err != nil {
		t.Fatalf("final apply: %v", err)
	}

	// 全部历史步骤与最终状态同基准逐项一致。
	got := r3.Steps()
	if len(got) != len(baseReps) {
		t.Fatalf("steps = %d, want %d", len(got), len(baseReps))
	}
	for i := range baseReps {
		if !reportsEqualForHistory(baseReps[i], got[i]) {
			t.Fatalf("step %d differs:\n base=%s\n got =%s", i, dumpStep(baseReps[i]), dumpStep(got[i]))
		}
	}
	if !snapshotsStrictEqual(baseRh.Snapshot(), r3.Snapshot()) {
		t.Fatalf("final snapshot differs from baseline")
	}
	t.Logf("结果: 失败步重试只生效一次（blow 恰好被抢 1 次、资源不多占），" +
		"续跑后失败/重试语义不变，全部历史与最终状态同基准逐项一致")
}
