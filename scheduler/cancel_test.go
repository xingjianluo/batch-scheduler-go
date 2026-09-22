package scheduler

import (
	"errors"
	"testing"
)

func cancelReason(t *testing.T, err error) CancelReason {
	t.Helper()
	var ce *CancelError
	if !errors.As(err, &ce) {
		t.Fatalf("want *CancelError, got %T: %v", err, err)
	}
	return ce.Reason
}

func TestCancelSemantics(t *testing.T) {
	s := newSched(t, baseConfig(t.TempDir()))
	mustSubmit(t, s, JobSpec{ID: "run", Tenant: "alpha", Priority: 1, Request: Resources{CPUMillis: 800, MemoryMB: 800}, DurationTicks: 10})
	mustSubmit(t, s, JobSpec{ID: "wait", Tenant: "alpha", Priority: 1, Request: Resources{CPUMillis: 800, MemoryMB: 800}, DurationTicks: 10})
	s.Tick() // run 运行（占满配额），wait 排队

	// 运行中取消：立即释放资源
	if err := s.Cancel("alpha", "run"); err != nil {
		t.Fatal(err)
	}
	if s.Job("run").State != StateCanceled {
		t.Fatalf("run=%s", s.Job("run").State)
	}
	s.Tick() // wait 应在 run 释放后立即得到资源
	if s.Job("wait").State != StateRunning {
		t.Fatalf("取消运行作业后 wait 应运行, got %s", s.Job("wait").State)
	}
	// 排队中取消
	mustSubmit(t, s, JobSpec{ID: "wait2", Tenant: "beta", Priority: 1, Request: Resources{CPUMillis: 100, MemoryMB: 100}, DurationTicks: 2})
	s.Tick()
	if err := s.Cancel("beta", "wait2"); err != nil {
		t.Fatal(err)
	}
	if s.Job("wait2").State != StateCanceled {
		t.Fatalf("wait2=%s", s.Job("wait2").State)
	}
	// 终态再取消 → TERMINAL
	if r := cancelReason(t, s.Cancel("alpha", "run")); r != CancelTerminal {
		t.Fatalf("got %s want TERMINAL", r)
	}
	// 不存在 → NOT_FOUND
	if r := cancelReason(t, s.Cancel("alpha", "ghost")); r != CancelNotFound {
		t.Fatalf("got %s want NOT_FOUND", r)
	}
	t.Logf("取消语义全部符合: run(CANCELED)->wait运行, wait2(CANCELED), used=%s", s.Snapshot().Used)

	// 重启后取消状态保持，且终态不被再调度
	dir := t.TempDir()
	s2 := newSched(t, baseConfig(dir))
	mustSubmit(t, s2, JobSpec{ID: "k", Tenant: "alpha", Priority: 1, Request: Resources{CPUMillis: 100, MemoryMB: 100}, DurationTicks: 5})
	s2.Tick()
	if err := s2.Cancel("alpha", "k"); err != nil {
		t.Fatal(err)
	}
	s2.Close()
	s3 := crashReopen(t, dir)
	for i := 0; i < 5; i++ {
		s3.Tick()
	}
	if s3.Job("k").State != StateCanceled || s3.Snapshot().Used.Any() {
		t.Fatalf("重启后取消状态丢失: k=%s used=%s", s3.Job("k").State, s3.Snapshot().Used)
	}
}
