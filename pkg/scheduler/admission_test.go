package scheduler

import (
	"errors"
	"testing"
)

// TestSubmitRejectReasons 校验五类拒绝原因可区分，且不产生任何占位。
func TestSubmitRejectReasons(t *testing.T) {
	tenants := []Tenant{
		{ID: "t-a", Quota: Resource{CPU: 4, Memory: 2048, GPU: 1}},
	}
	s, _ := newTestScheduler(t, Resource{CPU: 8, Memory: 8192, GPU: 4}, tenants)

	cases := []struct {
		name     string
		id       string
		tenant   string
		pri      int
		req      Resource
		wantKind RejectKind
	}{
		{"empty-id", "", "t-a", 0, Resource{CPU: 1}, RejectInvalidRequest},
		{"empty-tenant", "j0", "", 0, Resource{CPU: 1}, RejectInvalidRequest},
		{"negative-resource", "j1", "t-a", 0, Resource{CPU: -1}, RejectInvalidRequest},
		{"unknown-tenant", "j2", "t-x", 0, Resource{CPU: 1}, RejectTenantNotFound},
		{"over-cluster", "j3", "t-a", 0, Resource{CPU: 9}, RejectExceedsClusterCapacity},
		{"over-quota", "j4", "t-a", 0, Resource{CPU: 5}, RejectExceedsTenantQuota},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := s.Submit(c.id, c.tenant, c.pri, c.req)
			t.Logf("input id=%q tenant=%q req=%+v -> %s", c.id, c.tenant, c.req, fmtReject(err))
			if err == nil {
				t.Fatal("expected rejection, got nil")
			}
			var rj *RejectError
			if !errors.As(err, &rj) {
				t.Fatalf("want *RejectError, got %T", err)
			}
			if rj.Kind != c.wantKind {
				t.Fatalf("kind = %s, want %s", RejectKindName(rj.Kind), RejectKindName(c.wantKind))
			}
		})
	}
	logSnapshot(t, "after-rejections", s, nil)
	assertResource(t, s.Snapshot().Used, Resource{}, "cluster used after rejected submits")
}

// TestDuplicateSubmit 同一作业重复提交必须被拒绝，且不产生重复占位。
func TestDuplicateSubmit(t *testing.T) {
	tenants := []Tenant{{ID: "t-a", Quota: Resource{CPU: 2}}}
	s, _ := newTestScheduler(t, Resource{CPU: 4}, tenants)

	if _, err := s.Submit("dup", "t-a", 0, Resource{CPU: 1}); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	if _, err := s.Submit("dup", "t-a", 9, Resource{CPU: 2}); !IsReject(err, RejectDuplicateJob) {
		t.Fatalf("duplicate submit err = %s, want DuplicateJob", fmtReject(err))
	}
	snap := s.Snapshot()
	logSnapshot(t, "after-duplicate", s, nil)
	if n := countJobs(snap); n != 1 {
		t.Fatalf("job count = %d, want 1", n)
	}
	assertResource(t, snap.Used, Resource{CPU: 1}, "used must reflect only first submit")
	if len(snap.RunningIDs) != 1 {
		t.Fatalf("running = %v, want exactly 1", snap.RunningIDs)
	}

	// 终态作业 ID 同样不可复用。
	if _, err := s.Complete("dup"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if _, err := s.Submit("dup", "t-a", 0, Resource{CPU: 1}); !IsReject(err, RejectDuplicateJob) {
		t.Fatalf("reuse terminal id err = %s, want DuplicateJob", fmtReject(err))
	}
}

// TestQuotaBoundary 精确验证租户配额边界：恰好等于配额可准入，
// 超出一个最小单位即拒绝；租户之间互不借用。
func TestQuotaBoundary(t *testing.T) {
	tenants := []Tenant{
		{ID: "a", Quota: Resource{CPU: 4, Memory: 4096, GPU: 2}},
		{ID: "b", Quota: Resource{CPU: 2}},
	}
	s, _ := newTestScheduler(t, Resource{CPU: 16, Memory: 16384, GPU: 8}, tenants)

	// a 用满恰好等于配额：4 CPU。
	mustSubmit(t, s, "a1", "a", 0, Resource{CPU: 3, Memory: 4096, GPU: 2})
	r2, err := s.Submit("a2", "a", 0, Resource{CPU: 1})
	if err != nil {
		t.Fatalf("a2 (at exact quota boundary) rejected: %v", err)
	}
	if !r2.Started {
		t.Fatal("a2 should start: tenant usage exactly equals quota")
	}
	// 配额被运行中作业占满是暂时状态：a3 应受理排队（而非拒绝），
	// 且绝不启动（启动阶段的配额检查防止超卖）。
	r3, err := s.Submit("a3", "a", 0, Resource{CPU: 1})
	if err != nil {
		t.Fatalf("a3 should be admitted to queue: %s", fmtReject(err))
	}
	if r3.Started {
		t.Fatal("a3 must not start: tenant quota temporarily exhausted")
	}
	mustState(t, s, "a3", StateQueued)
	// 但超过静态配额上限的请求原则上永不可满足，仍直接拒绝。
	if _, err := s.Submit("a4", "a", 0, Resource{CPU: 5}); !IsReject(err, RejectExceedsTenantQuota) {
		t.Fatalf("a4 err = %s, want ExceedsTenantQuota", fmtReject(err))
	}
	// b 不能占用 a 的配额，但有自己独立的额度。
	mustSubmit(t, s, "b1", "b", 0, Resource{CPU: 2})
	rB2, err := s.Submit("b2", "b", 0, Resource{CPU: 1})
	if err != nil {
		t.Fatalf("b2 should be admitted to queue: %s", fmtReject(err))
	}
	if rB2.Started {
		t.Fatal("b2 must not start: tenant b quota temporarily exhausted")
	}
	// a1 完成释放后，a3 可启动（资源与配额同时满足）。
	if _, err := s.Complete("a1"); err != nil {
		t.Fatalf("complete a1: %v", err)
	}
	mustState(t, s, "a3", StateRunning)
	logSnapshot(t, "quota-boundary", s, nil)
	assertResource(t, s.Snapshot().TenantUsed["a"], Resource{CPU: 2}, "tenant a used after a1 completion")
	assertResource(t, s.Snapshot().TenantUsed["b"], Resource{CPU: 2}, "tenant b used")
}

// TestClusterCapacityBoundary 配额充足但集群容量不足时排队而非拒绝；
// 超过集群物理总容量才直接拒绝。
func TestClusterCapacityBoundary(t *testing.T) {
	tenants := []Tenant{{ID: "a", Quota: Resource{CPU: 100}}}
	s, _ := newTestScheduler(t, Resource{CPU: 4}, tenants)

	mustSubmit(t, s, "big1", "a", 0, Resource{CPU: 3})
	r2, err := s.Submit("big2", "a", 0, Resource{CPU: 2})
	if err != nil {
		t.Fatalf("big2 should be admitted to queue: %v", err)
	}
	if r2.Started {
		t.Fatal("big2 must stay queued: only 1 CPU free in cluster")
	}
	mustState(t, s, "big2", StateQueued)
	// 超出物理容量的作业没有任何可满足的时刻，提交即拒绝。
	_, err = s.Submit("impossible", "a", 0, Resource{CPU: 5})
	if !IsReject(err, RejectExceedsClusterCapacity) {
		t.Fatalf("impossible err = %s, want ExceedsClusterCapacity", fmtReject(err))
	}
	logSnapshot(t, "cluster-boundary", s, nil)
}

// TestMaxConcurrent 配额按资源量允许，但并发槽位受限时排队。
func TestMaxConcurrent(t *testing.T) {
	tenants := []Tenant{{ID: "a", Quota: Resource{CPU: 10}, MaxConcurrent: 1}}
	s, _ := newTestScheduler(t, Resource{CPU: 10}, tenants)
	mustSubmit(t, s, "only", "a", 0, Resource{CPU: 1})
	r, err := s.Submit("wait", "a", 0, Resource{CPU: 1})
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if r.Started {
		t.Fatal("wait must queue: tenant concurrent slot exhausted")
	}
	if _, err := s.Complete("only"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	mustState(t, s, "wait", StateRunning)
	logSnapshot(t, "max-concurrent", s, nil)
}

func mustSubmit(t *testing.T, s *Scheduler, id, tenant string, pri int, req Resource) {
	t.Helper()
	if _, err := s.Submit(id, tenant, pri, req); err != nil {
		t.Fatalf("Submit(%s): %s", id, fmtReject(err))
	}
}

func countJobs(snap *Snapshot) int { return len(snap.Jobs) }
