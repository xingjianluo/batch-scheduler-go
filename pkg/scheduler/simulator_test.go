package scheduler

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// --- 推演测试辅助 ---

// simTenants 是推演测试共用的租户配置。
func simTenants() []Tenant {
	return []Tenant{
		{ID: "a", Quota: Resource{CPU: 4, Memory: 4096, GPU: 1}},
		{ID: "b", Quota: Resource{CPU: 2, Memory: 4096}, MaxConcurrent: 1},
	}
}

func simTestConfig(workDir string, start time.Time) SimConfig {
	return SimConfig{
		Capacity:     Resource{CPU: 4, Memory: 4096, GPU: 1},
		AgeIncrement: 10,
		AgingPeriod:  10 * time.Second,
		WorkDir:      workDir,
		StartTime:    start,
	}
}

// simScript 构造覆盖 提交/完成/取消/推进/抢占/老化/各类拒绝 的事件脚本。
func simScript(start time.Time) []Event {
	t1 := start.Add(35 * time.Second)
	t2 := t1.Add(100 * time.Second)
	return []Event{
		SubmitEvent("j1", "a", 1, Resource{CPU: 2}), // 启动
		SubmitEvent("j2", "a", 1, Resource{CPU: 2}), // 启动，集群满
		SubmitEvent("j3", "a", 1, Resource{CPU: 1}), // 排队
		SubmitEvent("j4", "a", 5, Resource{CPU: 1}), // 抢占 j1
		AdvanceEvent(t1), // 老化后 j1 反超，抢占 j2
		SubmitEvent("j5", "b", 1, Resource{CPU: 1}),    // 触发级联
		SubmitEvent("j6", "b", 1, Resource{CPU: 1}),    // b 并发槽满，排队
		SubmitEvent("j1", "a", 1, Resource{CPU: 1}),    // 重复 ID 拒绝
		SubmitEvent("jx", "nope", 1, Resource{CPU: 1}), // 租户不存在拒绝
		SubmitEvent("jbig", "a", 1, Resource{CPU: 99}), // 超集群容量拒绝
		SubmitEvent("jq", "b", 1, Resource{CPU: 3}),    // 超租户配额拒绝
		CompleteEvent("j4"),                            // 释放但队首阻塞
		CompleteEvent("j1"),                            // 释放后级联启动
		CancelEvent("j6"),                              // 取消排队作业
		CompleteEvent("j5"),                            // 释放 b 槽位
		AdvanceEvent(t2),                               // 空队列推进
		CompleteEvent("ghost"),                         // 作业不存在（记录错误）
	}
}

// applyToReal 把同一事件应用到真实调度器（公共 API），返回可比对的结果。
func applyToReal(t *testing.T, s *Scheduler, clk *fakeClock, ev Event) (rejectKind, errStr string, decisions []Decision) {
	t.Helper()
	switch ev.Type {
	case EventSubmit:
		res, err := s.Submit(ev.JobID, ev.TenantID, ev.Priority, ev.Request)
		if err != nil {
			if re, ok := err.(*RejectError); ok {
				return RejectKindName(re.Kind), "", nil
			}
			return "", err.Error(), nil
		}
		return "", "", res.Decisions
	case EventComplete:
		d, err := s.Complete(ev.JobID)
		if err != nil {
			return "", err.Error(), nil
		}
		return "", "", []Decision{*d}
	case EventCancel:
		d, err := s.Cancel(ev.JobID)
		if err != nil {
			return "", err.Error(), nil
		}
		return "", "", []Decision{*d}
	case EventAdvance:
		clk.advance(ev.Time.Sub(clk.Now()))
		ds, err := s.Tick(ev.Time)
		if err != nil {
			return "", err.Error(), nil
		}
		return "", "", ds
	default:
		t.Fatalf("unknown event type %v", ev.Type)
		return "", "", nil
	}
}

// --- 逐项比较（时间用 Equal 比较，避免单调时钟/时区表示差异） ---

func assertTimeEqual(t *testing.T, what string, a, b time.Time) {
	t.Helper()
	if !a.Equal(b) {
		t.Fatalf("%s: time %v != %v", what, a, b)
	}
}

func assertJobEqual(t *testing.T, want, got Job) {
	t.Helper()
	if want.ID != got.ID || want.TenantID != got.TenantID ||
		want.Priority != got.Priority || want.Request != got.Request ||
		want.SubmitSeq != got.SubmitSeq || want.State != got.State ||
		want.PreemptedTimes != got.PreemptedTimes {
		t.Fatalf("job mismatch:\n want %+v\n got  %+v", want, got)
	}
	assertTimeEqual(t, want.ID+".SubmittedAt", want.SubmittedAt, got.SubmittedAt)
	assertTimeEqual(t, want.ID+".QueuedAt", want.QueuedAt, got.QueuedAt)
	assertTimeEqual(t, want.ID+".StartedAt", want.StartedAt, got.StartedAt)
	assertTimeEqual(t, want.ID+".FinishedAt", want.FinishedAt, got.FinishedAt)
}

func assertSnapshotEqual(t *testing.T, want, got *Snapshot) {
	t.Helper()
	assertTimeEqual(t, "Snapshot.Time", want.Time, got.Time)
	assertResource(t, got.Capacity, want.Capacity, "snapshot capacity")
	assertResource(t, got.Used, want.Used, "snapshot used")
	if len(want.Jobs) != len(got.Jobs) {
		t.Fatalf("snapshot job count = %d, want %d", len(got.Jobs), len(want.Jobs))
	}
	for i := range want.Jobs {
		assertJobEqual(t, want.Jobs[i], got.Jobs[i])
	}
	if len(want.QueuedIDs) != len(got.QueuedIDs) || len(want.RunningIDs) != len(got.RunningIDs) {
		t.Fatalf("snapshot queue/running = %v/%v, want %v/%v",
			got.QueuedIDs, got.RunningIDs, want.QueuedIDs, want.RunningIDs)
	}
	for i := range want.QueuedIDs {
		if want.QueuedIDs[i] != got.QueuedIDs[i] {
			t.Fatalf("queued[%d] = %s, want %s", i, got.QueuedIDs[i], want.QueuedIDs[i])
		}
	}
	for i := range want.RunningIDs {
		if want.RunningIDs[i] != got.RunningIDs[i] {
			t.Fatalf("running[%d] = %s, want %s", i, got.RunningIDs[i], want.RunningIDs[i])
		}
	}
	if len(want.Tenants) != len(got.Tenants) {
		t.Fatalf("snapshot tenants = %v, want %v", got.Tenants, want.Tenants)
	}
	for i := range want.Tenants {
		if want.Tenants[i] != got.Tenants[i] {
			t.Fatalf("tenant[%d] = %+v, want %+v", i, got.Tenants[i], want.Tenants[i])
		}
	}
	if len(want.TenantUsed) != len(got.TenantUsed) {
		t.Fatalf("snapshot tenantUsed = %v, want %v", got.TenantUsed, want.TenantUsed)
	}
	for id, w := range want.TenantUsed {
		if got.TenantUsed[id] != w {
			t.Fatalf("tenantUsed[%s] = %+v, want %+v", id, got.TenantUsed[id], w)
		}
	}
}

func assertDecisionEqual(t *testing.T, want, got Decision) {
	t.Helper()
	assertTimeEqual(t, "Decision.Time", want.Time, got.Time)
	if want.JobID != got.JobID || want.Action != got.Action || want.Reason != got.Reason ||
		want.JobState != got.JobState || want.Used != got.Used || want.Capacity != got.Capacity {
		t.Fatalf("decision mismatch:\n want %+v\n got  %+v", want, got)
	}
	if len(want.VictimIDs) != len(got.VictimIDs) {
		t.Fatalf("decision victims = %v, want %v", got.VictimIDs, want.VictimIDs)
	}
	for i := range want.VictimIDs {
		if want.VictimIDs[i] != got.VictimIDs[i] {
			t.Fatalf("decision victim[%d] = %s, want %s", i, got.VictimIDs[i], want.VictimIDs[i])
		}
	}
}

func assertDecisionsEqual(t *testing.T, want, got []Decision) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("decision count = %d, want %d\n got  %+v\n want %+v", len(got), len(want), got, want)
	}
	for i := range want {
		assertDecisionEqual(t, want[i], got[i])
	}
}

// assertSimInvariants 校验推演状态不越界：不超容量、不超配额、无重复调度。
func assertSimInvariants(t *testing.T, snap *Snapshot, tenants []Tenant) {
	t.Helper()
	if !snap.Capacity.Fits(snap.Used) {
		t.Fatalf("oversell: used=%+v capacity=%+v", snap.Used, snap.Capacity)
	}
	quota := map[string]Tenant{}
	for _, tn := range tenants {
		quota[tn.ID] = tn
	}
	running := map[string]bool{}
	for _, id := range snap.RunningIDs {
		running[id] = true
	}
	for _, id := range snap.QueuedIDs {
		if running[id] {
			t.Fatalf("job %s both queued and running", id)
		}
	}
	seen := map[string]bool{}
	var sum Resource
	tenantRun := map[string]int{}
	for _, j := range snap.Jobs {
		if seen[j.ID] {
			t.Fatalf("job %s appears twice", j.ID)
		}
		seen[j.ID] = true
		if j.State == StateRunning {
			sum = sum.Add(j.Request)
			tenantRun[j.TenantID]++
			tn := quota[j.TenantID]
			if tn.MaxConcurrent > 0 && tenantRun[j.TenantID] > tn.MaxConcurrent {
				t.Fatalf("tenant %s running %d > maxConcurrent %d", j.TenantID, tenantRun[j.TenantID], tn.MaxConcurrent)
			}
		}
	}
	if sum != snap.Used {
		t.Fatalf("account mismatch: sum=%+v used=%+v", sum, snap.Used)
	}
	for id, used := range snap.TenantUsed {
		if q, ok := quota[id]; ok && !q.Quota.Fits(used) {
			t.Fatalf("tenant %s over quota: used=%+v quota=%+v", id, used, q.Quota)
		}
	}
}

// TestSimulationMatchesRealRun 核心对齐测试：
// 同一事件脚本，推演结果必须与真实调度器逐步逐项一致
// （拒绝类别、操作错误、裁决链、每步后的完整快照）。
func TestSimulationMatchesRealRun(t *testing.T) {
	real, clk := newTestScheduler(t, Resource{CPU: 4, Memory: 4096, GPU: 1}, simTenants())

	sim, err := NewSimulator(simTestConfig("", clk.t), simTenants(), "")
	if err != nil {
		t.Fatalf("NewSimulator: %v", err)
	}
	t.Cleanup(func() { _ = sim.Close() })

	script := simScript(clk.t)
	for i, ev := range script {
		rec, err := sim.Apply(ev)
		if err != nil {
			t.Fatalf("step %d Apply: %v", i, err)
		}
		rk, es, decs := applyToReal(t, real, clk, ev)
		if rec.RejectKind != rk {
			t.Fatalf("step %d reject kind = %q, want %q", i, rec.RejectKind, rk)
		}
		if rec.Err != es {
			t.Fatalf("step %d err = %q, want %q", i, rec.Err, es)
		}
		switch ev.Type {
		case EventSubmit, EventAdvance:
			assertDecisionsEqual(t, decs, rec.Decisions)
		default: // 真实 Complete/Cancel 只返回首条裁决，比对首条即可
			if len(decs) > 0 {
				if len(rec.Decisions) == 0 {
					t.Fatalf("step %d: sim has no decisions, real has %+v", i, decs)
				}
				assertDecisionEqual(t, decs[0], rec.Decisions[0])
			}
		}
		assertSnapshotEqual(t, real.Snapshot(), rec.Snapshot)
		assertSimInvariants(t, rec.Snapshot, simTenants())
		t.Logf("step %2d %-8d job=%-5s reject=%-22q err=%-20q decisions=%d used=%+v",
			i, int(ev.Type), ev.JobID, rec.RejectKind, rec.Err, len(rec.Decisions), rec.Snapshot.Used)
	}
}

// TestSimulationLogicalTimeOnly 推演的结论只依赖逻辑时间：
// 同一事件脚本，瞬间跑完与穿插真实等待分多次跑，结论必须完全一致。
func TestSimulationLogicalTimeOnly(t *testing.T) {
	start := newFakeClock().t
	script := simScript(start)

	run := func(pace time.Duration) []StepRecord {
		sim, err := NewSimulator(simTestConfig("", start), simTenants(), "")
		if err != nil {
			t.Fatalf("NewSimulator: %v", err)
		}
		defer sim.Close()
		for _, ev := range script {
			if _, err := sim.Apply(ev); err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if pace > 0 {
				time.Sleep(pace) // 真实时间流逝不得影响结论
			}
		}
		return sim.Records()
	}

	fast := run(0)
	slow := run(3 * time.Millisecond)
	if len(fast) != len(slow) {
		t.Fatalf("record count %d != %d", len(fast), len(slow))
	}
	for i := range fast {
		if fast[i].RejectKind != slow[i].RejectKind || fast[i].Err != slow[i].Err {
			t.Fatalf("step %d outcome drift: (%q,%q) vs (%q,%q)",
				i, fast[i].RejectKind, fast[i].Err, slow[i].RejectKind, slow[i].Err)
		}
		assertDecisionsEqual(t, fast[i].Decisions, slow[i].Decisions)
		assertSnapshotEqual(t, fast[i].Snapshot, slow[i].Snapshot)
	}
}

// assertRecordEqual 逐项比较两步推演记录（含步后快照）。
func assertRecordEqual(t *testing.T, want, got StepRecord) {
	t.Helper()
	if want.Index != got.Index || want.RejectKind != got.RejectKind || want.Err != got.Err {
		t.Fatalf("record mismatch:\n want idx=%d reject=%q err=%q\n got  idx=%d reject=%q err=%q",
			want.Index, want.RejectKind, want.Err, got.Index, got.RejectKind, got.Err)
	}
	if want.Event != got.Event {
		// Event 含 time.Time，用 Equal 语义比较。
		w, g := want.Event, got.Event
		if w.Type != g.Type || w.JobID != g.JobID || w.TenantID != g.TenantID ||
			w.Priority != g.Priority || w.Request != g.Request || !w.Time.Equal(g.Time) {
			t.Fatalf("record %d event mismatch:\n want %+v\n got  %+v", want.Index, w, g)
		}
	}
	assertTimeEqual(t, "Record.Time", want.Time, got.Time)
	assertDecisionsEqual(t, want.Decisions, got.Decisions)
	assertSnapshotEqual(t, want.Snapshot, got.Snapshot)
}

// runSimScript 一口气跑完整个脚本，返回全部记录与最终快照（基准）。
func runSimScript(t *testing.T, cfg SimConfig, script []Event) ([]StepRecord, *Snapshot) {
	t.Helper()
	sim, err := NewSimulator(cfg, simTenants(), "")
	if err != nil {
		t.Fatalf("NewSimulator: %v", err)
	}
	t.Cleanup(func() { _ = sim.Close() })
	for i, ev := range script {
		if _, err := sim.Apply(ev); err != nil {
			t.Fatalf("apply step %d: %v", i, err)
		}
	}
	return sim.Records(), sim.Snapshot()
}

// TestSimulationResumeNoDrift 在脚本的每一个可能的中断点“杀掉”推演进程，
// 从检查点续跑，最终结论必须与一口气跑完完全一致（逐步记录逐项一致）。
func TestSimulationResumeNoDrift(t *testing.T) {
	start := newFakeClock().t
	script := simScript(start)

	// 基准：不中断跑完。
	refCfg := simTestConfig(t.TempDir(), start)
	wantRecords, wantSnap := runSimScript(t, refCfg, script)

	for cut := 0; cut <= len(script); cut++ {
		cut := cut
		t.Run(itoa(cut), func(t *testing.T) {
			dir := t.TempDir()
			cfg := simTestConfig(dir, start)
			// 第一段：跑到 cut 后模拟进程被杀（不 Close，直接丢弃）。
			func() {
				sim, err := NewSimulator(cfg, simTenants(), "")
				if err != nil {
					t.Fatalf("NewSimulator: %v", err)
				}
				for i := 0; i < cut; i++ {
					if _, err := sim.Apply(script[i]); err != nil {
						t.Fatalf("apply step %d before kill: %v", i, err)
					}
				}
				// 崩溃式退出：不调用 Close。
			}()
			// 第二段：从检查点恢复，跑完剩余事件。
			sim2, err := ResumeSimulator(cfg)
			if err != nil {
				t.Fatalf("ResumeSimulator: %v", err)
			}
			t.Cleanup(func() { _ = sim2.Close() })
			if got := len(sim2.Records()); got != cut {
				t.Fatalf("resumed records = %d, want %d", got, cut)
			}
			for i := cut; i < len(script); i++ {
				if _, err := sim2.Apply(script[i]); err != nil {
					t.Fatalf("apply step %d after resume: %v", i, err)
				}
			}
			gotRecords := sim2.Records()
			if len(gotRecords) != len(wantRecords) {
				t.Fatalf("record count = %d, want %d", len(gotRecords), len(wantRecords))
			}
			for i := range wantRecords {
				assertRecordEqual(t, wantRecords[i], gotRecords[i])
			}
			assertSnapshotEqual(t, wantSnap, sim2.Snapshot())
			assertSimInvariants(t, sim2.Snapshot(), simTenants())
		})
	}
}

// TestSimulationResumeMultipleKills 同一段脚本反复中断续跑多次，结论仍不漂移。
func TestSimulationResumeMultipleKills(t *testing.T) {
	start := newFakeClock().t
	script := simScript(start)
	_, wantSnap := runSimScript(t, simTestConfig(t.TempDir(), start), script)

	dir := t.TempDir()
	cfg := simTestConfig(dir, start)
	// 每 3 步杀一次，覆盖“抢占发生在中断边界前后”的组合。
	pos := 0
	first := true
	for pos < len(script) {
		var sim *Simulator
		var err error
		if first {
			sim, err = NewSimulator(cfg, simTenants(), "")
			first = false
		} else {
			sim, err = ResumeSimulator(cfg)
		}
		if err != nil {
			t.Fatalf("open at pos %d: %v", pos, err)
		}
		for i := 0; i < 3 && pos < len(script); i++ {
			if _, err := sim.Apply(script[pos]); err != nil {
				t.Fatalf("apply step %d: %v", pos, err)
			}
			pos++
		}
		// 崩溃式退出（不 Close）。
	}
	sim, err := ResumeSimulator(cfg)
	if err != nil {
		t.Fatalf("final resume: %v", err)
	}
	t.Cleanup(func() { _ = sim.Close() })
	assertSnapshotEqual(t, wantSnap, sim.Snapshot())
}

// --- 保存失败语义 ---

// failSimStore 可武装的检查点存储：failing 为 true 时 Save 必然失败。
type failSimStore struct {
	inner   simCheckpointStore
	failing bool
}

func (f *failSimStore) Save(cp *simCheckpoint) error {
	if f.failing {
		return errInjectedSave
	}
	return f.inner.Save(cp)
}

func (f *failSimStore) Load() (*simCheckpoint, error) { return f.inner.Load() }
func (f *failSimStore) Close() error                  { return f.inner.Close() }

var errInjectedSave = errors.New("injected checkpoint save failure")

func newFailStore(dir string) *failSimStore {
	return &failSimStore{inner: &fileSimStore{path: filepath.Join(dir, simCheckpointFile)}}
}

// TestSimulationSaveFailureSemantics 检查点落盘失败时：
// 本步对外等同于未发生（内存回滚、记录不增、重启后无痕迹），
// 原样重试不多占资源、队列不错位，最终结论与无故障基准一致。
func TestSimulationSaveFailureSemantics(t *testing.T) {
	start := newFakeClock().t
	script := simScript(start)
	// 基准：无故障跑完。
	wantRecords, wantSnap := runSimScript(t, simTestConfig(t.TempDir(), start), script)

	dir := t.TempDir()
	fs := newFailStore(dir)
	cfg := simTestConfig(dir, start)
	cfg.store = fs

	sim, err := NewSimulator(cfg, simTenants(), "")
	if err != nil {
		t.Fatalf("NewSimulator: %v", err)
	}
	t.Cleanup(func() { _ = sim.Close() })

	// 先正常跑 3 步（j1、j2 启动，j3 排队）。
	for i := 0; i < 3; i++ {
		if _, err := sim.Apply(script[i]); err != nil {
			t.Fatalf("apply step %d: %v", i, err)
		}
	}
	preSnap := sim.Snapshot()
	preRecords := sim.Records()

	// 第 3 步（j4 高优先级提交，会触发抢占）落盘失败。
	fs.failing = true
	if _, err := sim.Apply(script[3]); !errors.Is(err, errInjectedSave) {
		t.Fatalf("apply with failing store err = %v, want injected", err)
	}
	// 内存必须回滚：j1 仍在运行、j4 不存在、资源账不变。
	assertSnapshotEqual(t, preSnap, sim.Snapshot())
	if got := len(sim.Records()); got != len(preRecords) {
		t.Fatalf("records grew after failed step: %d -> %d", len(preRecords), got)
	}
	// 失败态下原样重试同一步：仍然失败，状态仍然不变（不重复生效）。
	if _, err := sim.Apply(script[3]); !errors.Is(err, errInjectedSave) {
		t.Fatalf("retry with failing store err = %v, want injected", err)
	}
	assertSnapshotEqual(t, preSnap, sim.Snapshot())

	// 重启后也看不到失败步骤的痕迹：检查点仍停在第 3 步之前。
	sim2, err := ResumeSimulator(cfg)
	if err != nil {
		t.Fatalf("ResumeSimulator: %v", err)
	}
	if got := len(sim2.Records()); got != 3 {
		t.Fatalf("resumed records = %d, want 3 (failed step must leave no trace)", got)
	}
	assertSnapshotEqual(t, preSnap, sim2.Snapshot())
	_ = sim2.Close()

	// 恢复落盘后原样重试同一步：成功且不多占资源，后续结论与基准一致。
	fs.failing = false
	rec, err := sim.Apply(script[3])
	if err != nil {
		t.Fatalf("retry after store recovered: %v", err)
	}
	assertRecordEqual(t, wantRecords[3], *rec)
	assertSimInvariants(t, rec.Snapshot, simTenants())
	for i := 4; i < len(script); i++ {
		if _, err := sim.Apply(script[i]); err != nil {
			t.Fatalf("apply step %d: %v", i, err)
		}
	}
	gotRecords := sim.Records()
	for i := range wantRecords {
		assertRecordEqual(t, wantRecords[i], gotRecords[i])
	}
	assertSnapshotEqual(t, wantSnap, sim.Snapshot())
}

// TestSimulationSaveFailureOnMutationStep 在“完成+级联调度”这类多步变更事件上
// 落盘失败，回滚必须恢复全部中间变更（释放的资源、被级联启动的作业）。
func TestSimulationSaveFailureOnMutationStep(t *testing.T) {
	start := newFakeClock().t
	script := simScript(start)
	// 找到 CompleteEvent("j1")（其完成会级联启动排队作业）。
	cut := -1
	for i, ev := range script {
		if ev.Type == EventComplete && ev.JobID == "j1" {
			cut = i
			break
		}
	}
	if cut < 0 {
		t.Fatal("script missing complete-j1 step")
	}

	dir := t.TempDir()
	fs := newFailStore(dir)
	cfg := simTestConfig(dir, start)
	cfg.store = fs
	sim, err := NewSimulator(cfg, simTenants(), "")
	if err != nil {
		t.Fatalf("NewSimulator: %v", err)
	}
	t.Cleanup(func() { _ = sim.Close() })
	for i := 0; i < cut; i++ {
		if _, err := sim.Apply(script[i]); err != nil {
			t.Fatalf("apply step %d: %v", i, err)
		}
	}
	preSnap := sim.Snapshot()

	fs.failing = true
	if _, err := sim.Apply(script[cut]); !errors.Is(err, errInjectedSave) {
		t.Fatalf("apply complete with failing store err = %v, want injected", err)
	}
	// j1 必须仍然 Running（完成的资源释放被回滚），级联启动也未发生。
	assertSnapshotEqual(t, preSnap, sim.Snapshot())
	fs.failing = false
	if _, err := sim.Apply(script[cut]); err != nil {
		t.Fatalf("retry complete: %v", err)
	}
	// 重试成功后的状态必须与无故障基准的同步一致。
	_, wantSnap := runSimScript(t, simTestConfig(t.TempDir(), start), script[:cut+1])
	assertSnapshotEqual(t, wantSnap, sim.Snapshot())
}

// TestSimulationInitCommitFailure 首个检查点落盘失败时，创建推演器本身失败。
func TestSimulationInitCommitFailure(t *testing.T) {
	dir := t.TempDir()
	fs := newFailStore(dir)
	fs.failing = true
	cfg := simTestConfig(dir, newFakeClock().t)
	cfg.store = fs
	if _, err := NewSimulator(cfg, simTenants(), ""); !errors.Is(err, errInjectedSave) {
		t.Fatalf("NewSimulator with failing store err = %v, want injected", err)
	}
}

// TestSimulatorEdgeCases 非法事件与生命周期边界。
func TestSimulatorEdgeCases(t *testing.T) {
	start := newFakeClock().t
	dir := t.TempDir()
	cfg := simTestConfig(dir, start)

	// 空目录上没有检查点可恢复。
	if _, err := ResumeSimulator(cfg); !errors.Is(err, ErrSimNoCheckpoint) {
		t.Fatalf("resume empty dir err = %v, want ErrSimNoCheckpoint", err)
	}

	sim, err := NewSimulator(cfg, simTenants(), "")
	if err != nil {
		t.Fatalf("NewSimulator: %v", err)
	}
	// 已有检查点的目录上重复创建。
	if _, err := NewSimulator(cfg, simTenants(), ""); !errors.Is(err, ErrSimCheckpointExists) {
		t.Fatalf("re-create err = %v, want ErrSimCheckpointExists", err)
	}
	// 时间倒退：报错且不消耗步号、不改变状态。
	preSnap := sim.Snapshot()
	if _, err := sim.Apply(AdvanceEvent(start.Add(-time.Second))); !errors.Is(err, ErrSimBackwardTime) {
		t.Fatalf("backward advance err = %v, want ErrSimBackwardTime", err)
	}
	if got := len(sim.Records()); got != 0 {
		t.Fatalf("backward advance consumed a step: records=%d", got)
	}
	assertSnapshotEqual(t, preSnap, sim.Snapshot())
	// 未知事件类型。
	if _, err := sim.Apply(Event{Type: EventType(99)}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("unknown event err = %v, want ErrInvalidArgument", err)
	}
	// 关闭后再应用。
	if err := sim.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := sim.Apply(SubmitEvent("x", "a", 1, Resource{CPU: 1})); !errors.Is(err, ErrSimClosed) {
		t.Fatalf("apply after close err = %v, want ErrSimClosed", err)
	}
}

// --- 出发点：已保存状态文件 / 运行中调度器 ---

// TestSimulationFromSavedStateFile 从既有调度器保存的状态文件出发推演：
// 与“真实重启该状态文件后继续操作”的结论逐项一致，且种子文件绝不回写。
func TestSimulationFromSavedStateFile(t *testing.T) {
	clk := newFakeClock()
	tenants := simTenants()
	cap_ := Resource{CPU: 4, Memory: 4096, GPU: 1}

	// 制造一个真实保存的状态点：运行/排队/终态作业齐全。
	seedDir := t.TempDir()
	seedPath := filepath.Join(seedDir, "state.json")
	func() {
		s := newPersistentScheduler(t, seedPath, clk, cap_, tenants)
		mustSubmit(t, s, "run-1", "a", 1, Resource{CPU: 2})
		mustSubmit(t, s, "run-2", "a", 1, Resource{CPU: 2})
		mustSubmit(t, s, "queue-1", "a", 1, Resource{CPU: 1})
		mustSubmit(t, s, "done-1", "a", 1, Resource{CPU: 1})
		if _, err := s.Complete("run-1"); err != nil { // 触发 queue-1 启动
			t.Fatalf("complete: %v", err)
		}
		if _, err := s.Cancel("done-1"); err != nil {
			t.Fatalf("cancel: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()
	seedBytes, err := os.ReadFile(seedPath)
	if err != nil {
		t.Fatalf("read seed: %v", err)
	}

	// 后续事件脚本（基于保存点继续）。
	script := []Event{
		SubmitEvent("new-1", "a", 5, Resource{CPU: 1}), // 高优先级，触发抢占
		AdvanceEvent(clk.t.Add(50 * time.Second)),      // 老化
		SubmitEvent("new-2", "b", 1, Resource{CPU: 1}),
		CompleteEvent("run-2"),
		CancelEvent("new-2"),
		AdvanceEvent(clk.t.Add(200 * time.Second)),
	}

	// 推演：从保存的状态文件出发（纯内存 + 检查点目录，不碰种子文件）。
	sim, err := NewSimulator(simTestConfig(t.TempDir(), clk.t), tenants, seedPath)
	if err != nil {
		t.Fatalf("NewSimulator from seed: %v", err)
	}
	t.Cleanup(func() { _ = sim.Close() })

	// 真跑对照：把同一状态文件复制到另一目录“真实重启”，喂同样事件。
	realDir := t.TempDir()
	realPath := filepath.Join(realDir, "state.json")
	if err := os.WriteFile(realPath, seedBytes, 0o644); err != nil {
		t.Fatalf("copy seed: %v", err)
	}
	real := newPersistentScheduler(t, realPath, clk, cap_, tenants)
	t.Cleanup(func() { _ = real.Close() })

	for i, ev := range script {
		rec, err := sim.Apply(ev)
		if err != nil {
			t.Fatalf("step %d Apply: %v", i, err)
		}
		rk, es, decs := applyToReal(t, real, clk, ev)
		if rec.RejectKind != rk || rec.Err != es {
			t.Fatalf("step %d outcome = (%q,%q), want (%q,%q)", i, rec.RejectKind, rec.Err, rk, es)
		}
		if ev.Type == EventSubmit || ev.Type == EventAdvance {
			assertDecisionsEqual(t, decs, rec.Decisions)
		}
		assertSnapshotEqual(t, real.Snapshot(), rec.Snapshot)
		assertSimInvariants(t, rec.Snapshot, tenants)
	}

	// 种子状态文件必须原封不动（推演绝不回写出发点）。
	after, err := os.ReadFile(seedPath)
	if err != nil {
		t.Fatalf("read seed after: %v", err)
	}
	if string(after) != string(seedBytes) {
		t.Fatal("seed state file was modified by simulation")
	}
}

// TestSimulationFromLiveScheduler 从运行中的调度器当前状态出发推演：
// 推演不打扰原调度器，且与同序真实操作的结论逐项一致。
func TestSimulationFromLiveScheduler(t *testing.T) {
	real, clk := newTestScheduler(t, Resource{CPU: 4, Memory: 4096, GPU: 1}, simTenants())
	mustSubmit(t, real, "live-1", "a", 1, Resource{CPU: 2})
	mustSubmit(t, real, "live-2", "a", 1, Resource{CPU: 2})
	mustSubmit(t, real, "live-3", "a", 1, Resource{CPU: 1}) // 排队

	sim, err := NewSimulatorFromScheduler(real, simTestConfig("", clk.t))
	if err != nil {
		t.Fatalf("NewSimulatorFromScheduler: %v", err)
	}

	// 推演里提交一个作业：原调度器必须毫无察觉。
	if _, err := sim.Apply(SubmitEvent("sim-only", "a", 9, Resource{CPU: 1})); err != nil {
		t.Fatalf("sim apply: %v", err)
	}
	if _, err := real.Get("sim-only"); err != ErrJobNotFound {
		t.Fatalf("live scheduler sees sim-only job: err=%v", err)
	}
	mustState(t, real, "live-1", StateRunning)
	mustState(t, real, "live-3", StateQueued)
	_ = sim.Close() // 丢弃已分叉的推演

	// 重新从原调度器当前状态导出一份干净推演，之后同序喂相同事件，
	// 推演与真跑必须逐步逐项一致。
	sim2, err := NewSimulatorFromScheduler(real, simTestConfig("", clk.t))
	if err != nil {
		t.Fatalf("NewSimulatorFromScheduler (2nd): %v", err)
	}
	t.Cleanup(func() { _ = sim2.Close() })
	script := []Event{
		SubmitEvent("x1", "a", 5, Resource{CPU: 1}),
		AdvanceEvent(clk.t.Add(40 * time.Second)),
		CompleteEvent("live-1"),
		CancelEvent("live-3"),
		AdvanceEvent(clk.t.Add(120 * time.Second)),
	}
	for i, ev := range script {
		rec, err := sim2.Apply(ev)
		if err != nil {
			t.Fatalf("step %d Apply: %v", i, err)
		}
		rk, es, decs := applyToReal(t, real, clk, ev)
		if rec.RejectKind != rk || rec.Err != es {
			t.Fatalf("step %d outcome = (%q,%q), want (%q,%q)", i, rec.RejectKind, rec.Err, rk, es)
		}
		if ev.Type == EventSubmit || ev.Type == EventAdvance {
			assertDecisionsEqual(t, decs, rec.Decisions)
		}
		assertSnapshotEqual(t, real.Snapshot(), rec.Snapshot)
		assertSimInvariants(t, rec.Snapshot, simTenants())
	}
}
