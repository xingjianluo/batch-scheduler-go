package scheduler

import (
	"errors"
	"io"
	"log"
	"sync"
	"testing"
)

// testLogger 把关键输入/快照/判定依据同时打印到测试日志（go test -v 可见）。
func testLogger(t *testing.T) *log.Logger {
	t.Helper()
	return log.New(io.Discard, "", 0)
}

func newSched(t *testing.T, cfg Config) *Scheduler {
	t.Helper()
	if cfg.Logger == nil {
		var mu sync.Mutex
		cfg.Logger = log.New(&testWriter{t: t, mu: &mu}, "[scheduler] ", log.LstdFlags|log.Lmicroseconds)
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

type testWriter struct {
	t  *testing.T
	mu *sync.Mutex
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.t.Logf("%s", p[:len(p)-1])
	return len(p), nil
}

func baseConfig(dir string) Config {
	return Config{
		Capacity: Resources{CPUMillis: 2000, MemoryMB: 2048},
		Tenants: []Tenant{
			{Name: "alpha", Quota: Resources{CPUMillis: 1000, MemoryMB: 1024}},
			{Name: "beta", Quota: Resources{CPUMillis: 2000, MemoryMB: 2048}},
		},
		DataDir: dir,
	}
}

func rejectReason(t *testing.T, err error) RejectReason {
	t.Helper()
	var se *SubmitError
	if !errors.As(err, &se) {
		t.Fatalf("want *SubmitError, got %T: %v", err, err)
	}
	t.Logf("拒绝原因=%s 消息=%q", se.Reason, se.Message)
	return se.Reason
}

// TestSubmitValidation 非法请求必须以 INVALID_REQUEST 拒绝。
func TestSubmitValidation(t *testing.T) {
	s := newSched(t, baseConfig(t.TempDir()))
	base := JobSpec{ID: "j", Tenant: "alpha", Priority: 1, Request: Resources{CPUMillis: 100, MemoryMB: 100}, DurationTicks: 1}

	cases := []struct {
		name string
		mut  func(*JobSpec)
	}{
		{"空ID", func(j *JobSpec) { j.ID = "" }},
		{"空租户", func(j *JobSpec) { j.Tenant = "" }},
		{"负优先级", func(j *JobSpec) { j.Priority = -1 }},
		{"零资源", func(j *JobSpec) { j.Request = Resources{} }},
		{"负CPU", func(j *JobSpec) { j.Request.CPUMillis = -1 }},
		{"零时长", func(j *JobSpec) { j.DurationTicks = 0 }},
		{"负时长", func(j *JobSpec) { j.DurationTicks = -3 }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			spec := base
			c.mut(&spec)
			_, _, err := s.Submit(spec)
			if err == nil {
				t.Fatalf("期望拒绝, 实际受理")
			}
			if r := rejectReason(t, err); r != ReasonInvalidRequest {
				t.Fatalf("reason=%s want INVALID_REQUEST", r)
			}
		})
	}
}

// TestSubmitUnknownTenant 未注册租户。
func TestSubmitUnknownTenant(t *testing.T) {
	s := newSched(t, baseConfig(t.TempDir()))
	_, _, err := s.Submit(JobSpec{ID: "j", Tenant: "ghost", Priority: 0, Request: Resources{CPUMillis: 10, MemoryMB: 10}, DurationTicks: 1})
	if r := rejectReason(t, err); r != ReasonUnknownTenant {
		t.Fatalf("reason=%s want UNKNOWN_TENANT", r)
	}
}

// TestSubmitCapacityFull 需求超过集群总容量（即使配额允许）。
func TestSubmitCapacityFull(t *testing.T) {
	cfg := baseConfig(t.TempDir())
	s := newSched(t, cfg)
	_, _, err := s.Submit(JobSpec{ID: "big", Tenant: "beta", Priority: 0,
		Request: Resources{CPUMillis: 2001, MemoryMB: 2048}, DurationTicks: 1})
	if r := rejectReason(t, err); r != ReasonCapacityFull {
		t.Fatalf("reason=%s want CAPACITY_FULL", r)
	}
	// 单维超限也拒绝（内存）
	_, _, err = s.Submit(JobSpec{ID: "big2", Tenant: "beta", Priority: 0,
		Request: Resources{CPUMillis: 100, MemoryMB: 99999}, DurationTicks: 1})
	if r := rejectReason(t, err); r != ReasonCapacityFull {
		t.Fatalf("reason=%s want CAPACITY_FULL (mem)", r)
	}
}

// TestSubmitQuotaExceeded 需求超过租户配额但未超集群容量。
func TestSubmitQuotaExceeded(t *testing.T) {
	s := newSched(t, baseConfig(t.TempDir()))
	// alpha 配额 1000/1024；beta 配额 2000/2048
	_, _, err := s.Submit(JobSpec{ID: "q", Tenant: "alpha", Priority: 0,
		Request: Resources{CPUMillis: 1001, MemoryMB: 100}, DurationTicks: 1})
	if r := rejectReason(t, err); r != ReasonQuotaExceeded {
		t.Fatalf("reason=%s want QUOTA_EXCEEDED", r)
	}
}

// TestSubmitIdempotent 同一 ID 重复提交不重复占位。
func TestSubmitIdempotent(t *testing.T) {
	s := newSched(t, baseConfig(t.TempDir()))
	spec := JobSpec{ID: "dup", Tenant: "alpha", Priority: 1, Request: Resources{CPUMillis: 500, MemoryMB: 512}, DurationTicks: 2}
	j1, created1, err := s.Submit(spec)
	if err != nil || !created1 {
		t.Fatalf("first submit created=%v err=%v", created1, err)
	}
	// 用不同参数重复提交：返回已有作业，参数被忽略，不新增占位
	spec2 := spec
	spec2.Priority = 99
	spec2.Request = Resources{CPUMillis: 900, MemoryMB: 900}
	j2, created2, err := s.Submit(spec2)
	if err != nil || created2 {
		t.Fatalf("duplicate submit created=%v err=%v", created2, err)
	}
	if j1.ID != j2.ID || j2.Priority != 1 || j2.Request != (Resources{CPUMillis: 500, MemoryMB: 512}) {
		t.Fatalf("重复提交必须返回原作业, got %+v", j2)
	}
	s.Tick()
	snap := s.Snapshot()
	t.Logf("资源快照: used=%s/%s, alpha used=%s", snap.Used, snap.Capacity, snap.Tenants[0].Used)
	if n := countJobs(snap); n != 1 {
		t.Fatalf("作业数=%d, 期望 1（重复提交不得占位）", n)
	}
	if snap.Used != (Resources{CPUMillis: 500, MemoryMB: 512}) {
		t.Fatalf("used=%s, 期望仅一次 500/512", snap.Used)
	}
}

// TestQuotaBoundaryRunning 配额边界：租户已用+需求<=配额可准入运行，超出则排队（不违反提交期静态可行性）。
func TestQuotaBoundaryRunning(t *testing.T) {
	s := newSched(t, baseConfig(t.TempDir()))
	submit := func(id string, cpu, mem int64) {
		t.Helper()
		if _, _, err := s.Submit(JobSpec{ID: id, Tenant: "alpha", Priority: 1, Request: Resources{CPUMillis: cpu, MemoryMB: mem}, DurationTicks: 3}); err != nil {
			t.Fatal(err)
		}
	}
	// alpha 配额恰好 1000/1024：先占 600，再放 400（边界恰好可容纳），第三个必须排队
	submit("a1", 600, 600)
	submit("a2", 400, 424)
	s.Tick()
	snap := s.Snapshot()
	t.Logf("边界快照 tick=%d: alpha used=%s/%s running=%v pending=%v",
		snap.Tick, snap.Tenants[0].Used, snap.Tenants[0].Quota, snap.Tenants[0].Running, snap.Tenants[0].Pending)
	if got := snap.Tenants[0].Used; got != (Resources{CPUMillis: 1000, MemoryMB: 1024}) {
		t.Fatalf("配额用满边界 used=%s", got)
	}
	submit("a3", 1, 0) // 需求合法但配额已满 → 排队
	s.Tick()
	snap = s.Snapshot()
	t.Logf("满载后快照: alpha used=%s running=%v pending=%v", snap.Tenants[0].Used, snap.Tenants[0].Running, snap.Tenants[0].Pending)
	if s.Job("a3").State != StatePending {
		t.Fatalf("a3 应排队, got %s", s.Job("a3").State)
	}
	if snap.Used.CPUMillis > 2000 || snap.Tenants[0].Used.CPUMillis > 1000 {
		t.Fatalf("发生超卖: cluster=%s tenant=%s", snap.Used, snap.Tenants[0].Used)
	}
}

// TestRejectReasonsDistinct 各拒绝原因可区分、且优先级判定顺序稳定。
func TestRejectReasonsDistinct(t *testing.T) {
	s := newSched(t, baseConfig(t.TempDir()))
	// 同一请求不会同时命中多个原因；分别构造并断言原因集合互不混淆。
	reasons := map[RejectReason]bool{}
	_, err1 := sSubmit(s, JobSpec{ID: "", Tenant: "alpha", Priority: 0, Request: Resources{CPUMillis: 1, MemoryMB: 1}, DurationTicks: 1})
	reasons[rejectReason(t, err1)] = true
	_, err2 := sSubmit(s, JobSpec{ID: "x", Tenant: "nope", Priority: 0, Request: Resources{CPUMillis: 1, MemoryMB: 1}, DurationTicks: 1})
	reasons[rejectReason(t, err2)] = true
	_, err3 := sSubmit(s, JobSpec{ID: "x", Tenant: "beta", Priority: 0, Request: Resources{CPUMillis: 99999, MemoryMB: 1}, DurationTicks: 1})
	reasons[rejectReason(t, err3)] = true
	_, err4 := sSubmit(s, JobSpec{ID: "x", Tenant: "alpha", Priority: 0, Request: Resources{CPUMillis: 1500, MemoryMB: 1}, DurationTicks: 1})
	reasons[rejectReason(t, err4)] = true
	for _, want := range []RejectReason{ReasonInvalidRequest, ReasonUnknownTenant, ReasonCapacityFull, ReasonQuotaExceeded} {
		if !reasons[want] {
			t.Fatalf("缺少可区分原因 %s", want)
		}
	}
}

func sSubmit(s *Scheduler, spec JobSpec) (*Job, error) {
	j, _, err := s.Submit(spec)
	return j, err
}

func countJobs(snap StateSnapshot) int { return len(snap.Jobs) }
