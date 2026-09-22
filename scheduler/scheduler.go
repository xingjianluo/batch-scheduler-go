package scheduler

import (
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
)

// Config 调度器配置。
type Config struct {
	// Capacity 集群总资源（进程内资源池模拟集群）。
	Capacity Resources
	// Tenants 冷启动时注册的租户及配额；从 WAL 恢复时以日志为准。
	Tenants []Tenant
	// DataDir 非空则使用 <DataDir>/wal.jsonl 做文件持久化；为空则纯内存。
	DataDir string
	// Logger 诊断日志，为空时写入 stderr。
	Logger *log.Logger
}

// TickResult 一个调度 tick 的确定性结果。
type TickResult struct {
	Tick      int64
	Started   []string // 本 tick 新开始/恢复运行的作业
	Preempted []string // 本 tick 被抢占回退的作业
	Completed []string // 本 tick 完成的作业
	Canceled  []string // 本 tick 生效取消的作业
}

// TenantSnapshot 租户视角的资源快照。
type TenantSnapshot struct {
	Name    string
	Quota   Resources
	Used    Resources
	Running []string
	Pending []string
}

// StateSnapshot 某一时刻调度器全量状态（供观测与测试断言）。
type StateSnapshot struct {
	Tick     int64
	Capacity Resources
	Used     Resources
	Tenants  []TenantSnapshot
	Jobs     []Job
}

// Scheduler 本地批处理作业调度器。
// 并发模型：单一互斥锁串行化所有状态变更（提交/取消/tick），
// 因此不会出现资源超卖或同一作业被重复调度。
type Scheduler struct {
	mu          sync.Mutex
	cfg         Config
	store       Store
	log         *log.Logger
	tick        int64
	seq         int64
	jobs        map[string]*Job
	tenants     map[string]*Tenant
	tenantOrder []string
	used        map[string]Resources // 各租户已占用资源
	usedTotal   Resources            // 全集群已占用资源
}

// New 构造调度器。DataDir 非空时打开文件 WAL 并重放恢复；
// 重放完成后，已受理作业的排队、占位与终态与崩溃前完全一致。
func New(cfg Config) (*Scheduler, error) {
	lg := cfg.Logger
	if lg == nil {
		lg = log.New(os.Stderr, "[scheduler] ", log.LstdFlags|log.Lmicroseconds)
	}
	s := &Scheduler{
		cfg:     cfg,
		log:     lg,
		jobs:    map[string]*Job{},
		tenants: map[string]*Tenant{},
		used:    map[string]Resources{},
	}
	if cfg.DataDir != "" {
		store, err := NewFileStore(cfg.DataDir + "/wal.jsonl")
		if err != nil {
			return nil, err
		}
		s.store = store
	} else {
		s.store = NewMemoryStore()
	}

	// 重放历史事件恢复状态。
	events, err := s.store.Read()
	if err != nil {
		return nil, fmt.Errorf("recover wal: %w", err)
	}
	for _, ev := range events {
		s.apply(ev, false)
	}
	if len(events) > 0 {
		s.reconcileAfterReplay()
		s.log.Printf("恢复完成: 重放 %d 条事件, tick=%d, 作业数=%d", len(events), s.tick, len(s.jobs))
	} else {
		// 冷启动：注册配置中的租户。
		for _, t := range cfg.Tenants {
			if err := s.RegisterTenant(t); err != nil {
				return nil, err
			}
		}
		s.log.Printf("冷启动: 集群容量 %s, 租户数=%d", cfg.Capacity, len(cfg.Tenants))
	}
	return s, nil
}

// reconcileAfterReplay 在 WAL 重放结束后修正"崩溃于运行片段中途"的作业：
// 这些作业最后一条事件是 DISPATCHED，其 Remaining 仍是派发时的值，
// 需按崩溃时刻（当前 tick）已经实际运行的 tick 数折算剩余时长，
// 并把 FinishTick 平移到恢复后的预计完成点，保证重启后不会重复运行。
func (s *Scheduler) reconcileAfterReplay() {
	// 时间模型（全局统一）：作业在 tick N 派发，占用时间片 N..N+R-1（共 R 个），
	// 在 tick N+R 判定完成。
	//   - 抢占发生在 tick T：tick T 时间片被抢走，已运行 T-N 片（见 EvPreempted）；
	//   - 崩溃发生在 tick T 的处理完成之后：tick T 时间片已正常跑完，
	//     已消耗 T-N+1 片，故剩余 R-(T-N+1)。
	// 恢复后预计完成点平移到当前 tick 的下一 tick + 剩余。
	for _, j := range s.jobs {
		if j.State != StateRunning {
			continue
		}
		consumed := s.tick - j.StartTick + 1
		if consumed < 0 {
			consumed = 0
		}
		j.Remaining -= consumed
		if j.Remaining < 0 {
			j.Remaining = 0
		}
		// 重启后：下一 tick(T+1) 继续运行剩余片，完成点 = (T+1)+剩余。
		j.StartTick = s.tick + 1
		j.FinishTick = j.StartTick + j.Remaining
	}
}

// nextSeq 必须在持锁状态下调用。
func (s *Scheduler) nextSeq() int64 {
	s.seq++
	return s.seq
}

// apply 将一条事件应用到内存状态。persist=false 用于 WAL 重放；
// persist=true 时事件已在日志中，仅更新内存（由调用方先 Append）。
func (s *Scheduler) apply(ev *Event, _ bool) {
	if ev.Seq > s.seq {
		s.seq = ev.Seq
	}
	switch ev.Type {
	case EvTenantRegistered:
		if _, ok := s.tenants[ev.Tenant]; !ok {
			s.tenants[ev.Tenant] = &Tenant{Name: ev.Tenant, Quota: ev.Quota}
			s.tenantOrder = append(s.tenantOrder, ev.Tenant)
			s.used[ev.Tenant] = Resources{}
		}
	case EvSubmitted:
		if _, ok := s.jobs[ev.JobID]; ok {
			break // 幂等：重放重复提交不产生重复占位
		}
		s.jobs[ev.JobID] = &Job{
			ID: ev.JobID, Tenant: ev.Tenant, Priority: ev.Priority,
			Request: ev.Request, Duration: ev.Duration, Remaining: ev.Duration,
			State: StatePending, EnqueueTick: ev.Tick,
		}
	case EvTick:
		if ev.Tick > s.tick {
			s.tick = ev.Tick
		}
	case EvDispatched:
		j := s.jobs[ev.JobID]
		if j == nil {
			break
		}
		// 从排队态转入运行（若重放的是抢占后的再次调度，
		// PREEMPTED 事件已先把它置为 PENDING）。
		if j.State != StateRunning {
			s.used[j.Tenant] = s.used[j.Tenant].Add(j.Request)
			s.usedTotal = s.usedTotal.Add(j.Request)
		}
		j.State = StateRunning
		j.StartTick = ev.StartTick
		j.FinishTick = ev.FinishTick
		j.StartEffPrio = ev.EffPriority
		// 派发事件记录的是本次运行片段开始时的剩余时长；
		// 若崩溃发生在运行片段中途，折算在重放结束后的 reconcile 中统一完成。
		j.Remaining = ev.Remaining
		j.Runs++
	case EvPreempted:
		j := s.jobs[ev.JobID]
		if j == nil || j.State != StateRunning {
			break
		}
		// 按已经实际运行的 tick 数扣减剩余时长。
		ran := ev.Tick - j.StartTick
		if ran < 0 {
			ran = 0
		}
		j.Remaining -= ran
		if j.Remaining < 0 {
			j.Remaining = 0
		}
		s.used[j.Tenant] = s.used[j.Tenant].Sub(j.Request)
		s.usedTotal = s.usedTotal.Sub(j.Request)
		j.State = StatePending
		j.StartTick = 0
		j.FinishTick = 0
		j.EnqueueTick = ev.NextEnqueue // 下一 tick 才可重新参与调度
		j.Preempted++
	case EvCompleted:
		j := s.jobs[ev.JobID]
		if j == nil || j.State == StateCompleted {
			break
		}
		if j.State == StateRunning {
			s.used[j.Tenant] = s.used[j.Tenant].Sub(j.Request)
			s.usedTotal = s.usedTotal.Sub(j.Request)
		}
		j.State = StateCompleted
		j.Remaining = 0
		j.EndTick = ev.Tick
		j.StartTick = 0
		j.FinishTick = 0
	case EvCanceled:
		j := s.jobs[ev.JobID]
		if j == nil || j.State.Terminal() {
			break
		}
		if j.State == StateRunning {
			s.used[j.Tenant] = s.used[j.Tenant].Sub(j.Request)
			s.usedTotal = s.usedTotal.Sub(j.Request)
		}
		j.State = StateCanceled
		j.EndTick = ev.Tick
		j.StartTick = 0
		j.FinishTick = 0
	}
}

// emit 先持久化再应用（持锁调用）。
func (s *Scheduler) emit(ev *Event) error {
	ev.Seq = s.nextSeq()
	if err := s.store.Append(ev); err != nil {
		return err
	}
	s.apply(ev, true)
	return nil
}

// RegisterTenant 注册租户及其配额（幂等：已存在则返回错误以暴露配置冲突）。
func (s *Scheduler) RegisterTenant(t Tenant) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.Name == "" {
		return fmt.Errorf("tenant name is empty")
	}
	if _, ok := s.tenants[t.Name]; ok {
		return fmt.Errorf("tenant %q already registered", t.Name)
	}
	return s.emit(&Event{Tick: s.tick, Type: EvTenantRegistered, Tenant: t.Name, Quota: t.Quota})
}

// Submit 提交作业。
// 拒绝原因（可区分）：INVALID_REQUEST / UNKNOWN_TENANT / QUOTA_EXCEEDED / CAPACITY_FULL。
// 幂等：同一 ID 重复提交直接返回已有作业快照（created=false, err=nil），不重复占位。
func (s *Scheduler) Submit(spec JobSpec) (*Job, bool, error) {
	// 1) 请求合法性
	if spec.ID == "" {
		return nil, false, &SubmitError{ReasonInvalidRequest, "job id is empty"}
	}
	if spec.Tenant == "" {
		return nil, false, &SubmitError{ReasonInvalidRequest, fmt.Sprintf("job %s: tenant is empty", spec.ID)}
	}
	if spec.Priority < 0 {
		return nil, false, &SubmitError{ReasonInvalidRequest, fmt.Sprintf("job %s: priority must be >= 0, got %d", spec.ID, spec.Priority)}
	}
	if !spec.Request.Any() || spec.Request.CPUMillis < 0 || spec.Request.MemoryMB < 0 {
		return nil, false, &SubmitError{ReasonInvalidRequest, fmt.Sprintf("job %s: resource request must be positive, got %s", spec.ID, spec.Request)}
	}
	if spec.DurationTicks <= 0 {
		return nil, false, &SubmitError{ReasonInvalidRequest, fmt.Sprintf("job %s: duration must be > 0, got %d", spec.ID, spec.DurationTicks)}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// 2) 重复提交：幂等返回已有作业
	if existing, ok := s.jobs[spec.ID]; ok {
		s.log.Printf("提交幂等命中: job=%s state=%s（不重复占位）", spec.ID, existing.State)
		return existing.Clone(), false, nil
	}

	// 3) 租户是否存在
	t, ok := s.tenants[spec.Tenant]
	if !ok {
		return nil, false, &SubmitError{ReasonUnknownTenant, fmt.Sprintf("job %s: tenant %q not registered", spec.ID, spec.Tenant)}
	}

	// 4) 容量/配额的静态可行性：需求永远无法满足则直接拒绝
	if !spec.Request.FitsIn(s.cfg.Capacity) {
		s.log.Printf("拒绝提交(CAPACITY_FULL): job=%s demand=%s capacity=%s", spec.ID, spec.Request, s.cfg.Capacity)
		return nil, false, &SubmitError{ReasonCapacityFull, fmt.Sprintf("job %s: request %s exceeds cluster capacity %s", spec.ID, spec.Request, s.cfg.Capacity)}
	}
	if !spec.Request.FitsIn(t.Quota) {
		s.log.Printf("拒绝提交(QUOTA_EXCEEDED): job=%s tenant=%s demand=%s quota=%s", spec.ID, spec.Tenant, spec.Request, t.Quota)
		return nil, false, &SubmitError{ReasonQuotaExceeded, fmt.Sprintf("job %s: request %s exceeds tenant %q quota %s", spec.ID, spec.Request, spec.Tenant, t.Quota)}
	}

	// 5) 受理：先持久化后入内存
	ev := &Event{Tick: s.tick, Type: EvSubmitted, JobID: spec.ID, Tenant: spec.Tenant,
		Priority: spec.Priority, Request: spec.Request, Duration: spec.DurationTicks}
	if err := s.emit(ev); err != nil {
		return nil, false, fmt.Errorf("persist submit: %w", err)
	}
	s.log.Printf("已受理: job=%s tenant=%s prio=%d demand=%s duration=%d tick=%d",
		spec.ID, spec.Tenant, spec.Priority, spec.Request, spec.DurationTicks, s.tick)
	return s.jobs[spec.ID].Clone(), true, nil
}

// Cancel 取消排队中或运行中的作业。运行中的作业立即释放资源。
// 终态/不存在返回可区分的 CancelError。
func (s *Scheduler) Cancel(tenant, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return &CancelError{CancelNotFound, fmt.Sprintf("job %q not found", id)}
	}
	if j.Tenant != tenant {
		return &CancelError{CancelNotFound, fmt.Sprintf("job %q does not belong to tenant %q", id, tenant)}
	}
	if j.State.Terminal() {
		return &CancelError{CancelTerminal, fmt.Sprintf("job %s already in terminal state %s", id, j.State)}
	}
	if err := s.emit(&Event{Tick: s.tick, Type: EvCanceled, JobID: id, Tenant: tenant}); err != nil {
		return fmt.Errorf("persist cancel: %w", err)
	}
	s.log.Printf("已取消: job=%s tenant=%s state-before release, tick=%d, 释放=%s", id, tenant, s.tick, j.Request)
	return nil
}

// Tick 推进一个时间单位。顺序固定（保证可重放/确定性）：
//  1. 持久化并进入新 tick；
//  2. 运行中作业到达 FinishTick 的标记完成并释放资源；
//  3. 取消在本 tick 之前请求且尚未生效的运行作业（Cancel 是即时生效，
//     故此处无需扫描；取消在调用瞬间已处理）；
//  4. 调度轮：按确定性顺序处理排队作业，必要时抢占低优先级作业。
func (s *Scheduler) Tick() *TickResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	res := &TickResult{Tick: s.tick + 1, Started: []string{}, Preempted: []string{}, Completed: []string{}, Canceled: []string{}}

	// 1) 推进 tick（先持久化，保证崩溃恢复后 tick 不错位）
	if err := s.emit(&Event{Tick: res.Tick, Type: EvTick}); err != nil {
		s.log.Printf("fatal: persist tick failed: %v", err)
		return res
	}

	// 2) 完成到期作业（按 ID 排序，保证确定性）
	var running []*Job
	for _, j := range s.jobs {
		if j.State == StateRunning {
			running = append(running, j)
		}
	}
	sort.Slice(running, func(i, k int) bool { return running[i].ID < running[k].ID })
	for _, j := range running {
		if j.FinishTick <= s.tick {
			if err := s.emit(&Event{Tick: s.tick, Type: EvCompleted, JobID: j.ID, Tenant: j.Tenant}); err != nil {
				s.log.Printf("fatal: persist complete failed: %v", err)
				return res
			}
			res.Completed = append(res.Completed, j.ID)
			s.log.Printf("完成: job=%s tick=%d 释放=%s", j.ID, s.tick, j.Request)
		}
	}

	// 3) 调度轮
	res.Started, res.Preempted = s.schedulePass()

	s.log.Printf("tick=%d 完成 快照: used=%s/%s started=%v preempted=%v completed=%v",
		s.tick, s.usedTotal, s.cfg.Capacity, res.Started, res.Preempted, res.Completed)
	return res
}

// schedulePass 一轮确定性调度（持锁调用）。
func (s *Scheduler) schedulePass() (started, preempted []string) {
	started, preempted = []string{}, []string{}

	// 本 tick 可参与调度的排队作业：被抢占者下一 tick 才可重新入队。
	var candidates []*Job
	for _, j := range s.jobs {
		if j.State == StatePending && j.EnqueueTick <= s.tick {
			candidates = append(candidates, j)
		}
	}
	// 确定性顺序：有效优先级降序；平手按基础优先级、入队 tick、作业 ID。
	sort.Slice(candidates, func(i, k int) bool {
		a, b := candidates[i], candidates[k]
		ea, eb := a.EffectivePriority(s.tick), b.EffectivePriority(s.tick)
		if ea != eb {
			return ea > eb
		}
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		if a.EnqueueTick != b.EnqueueTick {
			return a.EnqueueTick < b.EnqueueTick
		}
		return a.ID < b.ID
	})

	for _, job := range candidates {
		if job.State != StatePending {
			continue // 可能已在本 tick 被更前面的逻辑影响（正常不会，防御性判断）
		}
		eff := job.EffectivePriority(s.tick)
		tenantUsed := s.used[job.Tenant]

		// 无需抢占直接可行？
		capFree := s.cfg.Capacity.Sub(s.usedTotal)
		quotaFree := s.tenants[job.Tenant].Quota.Sub(tenantUsed)
		if job.Request.FitsIn(capFree) && job.Request.FitsIn(quotaFree) {
			s.dispatch(job, eff)
			started = append(started, job.ID)
			continue
		}

		// 需要抢占：枚举可抢占的运行作业（冻结有效优先级严格更低）。
		victims := s.pickVictims(job, eff, capFree, quotaFree)
		if victims == nil {
			s.log.Printf("等待: job=%s effPrio=%d demand=%s capFree=%s quotaFree=%s — 无可抢占资源，继续排队",
				job.ID, eff, job.Request, capFree, quotaFree)
			continue
		}
		for _, v := range victims {
			if err := s.emit(&Event{Tick: s.tick, Type: EvPreempted, JobID: v.ID, Tenant: v.Tenant,
				StartTick: s.tick, NextEnqueue: s.tick + 1, Cause: fmt.Sprintf("preempted by %s", job.ID)}); err != nil {
				s.log.Printf("fatal: persist preempt failed: %v", err)
				return started, preempted
			}
			preempted = append(preempted, v.ID)
			s.log.Printf("抢占: victim=%s(victimEff=%d) 被 job=%s(waitEff=%d) 抢占, 下一tick重新入队",
				v.ID, v.StartEffPrio, job.ID, eff)
		}
		// 抢占后资源应已足够；dispatch 内部无额外判定（调用方已确保）。
		s.dispatch(job, eff)
		started = append(started, job.ID)
	}
	return started, preempted
}

// pickVictims 贪心选择一组被抢占者，使集群空闲与租户空闲都能容纳 req。
// 选择规则确定性：冻结有效优先级升序（最低先牺牲），平手按开始 tick、作业 ID。
// 返回 nil 表示即使抢占所有可抢占者也无法满足（此时不抢占任何作业）。
func (s *Scheduler) pickVictims(job *Job, waitEff int64, capFree, quotaFree Resources) []*Job {
	var eligible []*Job
	for _, v := range s.jobs {
		if v.State != StateRunning || v.ID == job.ID {
			continue
		}
		if v.StartEffPrio >= waitEff {
			continue // 只抢占严格更低的（冻结）有效优先级
		}
		eligible = append(eligible, v)
	}
	sort.Slice(eligible, func(i, k int) bool {
		a, b := eligible[i], eligible[k]
		if a.StartEffPrio != b.StartEffPrio {
			return a.StartEffPrio < b.StartEffPrio
		}
		if a.StartTick != b.StartTick {
			return a.StartTick < b.StartTick
		}
		return a.ID < b.ID
	})

	needCap := Resources{ // 集群维度还缺多少
		CPUMillis: pos(job.Request.CPUMillis - capFree.CPUMillis),
		MemoryMB:  pos(job.Request.MemoryMB - capFree.MemoryMB),
	}
	needQuota := Resources{ // 租户配额维度还缺多少
		CPUMillis: pos(job.Request.CPUMillis - quotaFree.CPUMillis),
		MemoryMB:  pos(job.Request.MemoryMB - quotaFree.MemoryMB),
	}

	var chosen []*Job
	for _, v := range eligible {
		if !needCap.Any() && !needQuota.Any() {
			break
		}
		// 同租户牺牲者同时缓解容量与配额；跨租户只缓解容量。
		reliefCap := v.Request
		reliefQuota := Resources{}
		if v.Tenant == job.Tenant {
			reliefQuota = v.Request
		}
		// 该牺牲者对任一仍存在的缺口都无贡献时跳过（避免无意义抢占）。
		helpsCap := needCap.Any() && (reliefCap.CPUMillis > 0 || reliefCap.MemoryMB > 0)
		helpsQuota := needQuota.Any() && (reliefQuota.CPUMillis > 0 || reliefQuota.MemoryMB > 0)
		if !helpsCap && !helpsQuota {
			continue
		}
		chosen = append(chosen, v)
		needCap.CPUMillis = pos(needCap.CPUMillis - reliefCap.CPUMillis)
		needCap.MemoryMB = pos(needCap.MemoryMB - reliefCap.MemoryMB)
		needQuota.CPUMillis = pos(needQuota.CPUMillis - reliefQuota.CPUMillis)
		needQuota.MemoryMB = pos(needQuota.MemoryMB - reliefQuota.MemoryMB)
	}
	if needCap.Any() || needQuota.Any() {
		return nil // 抢占全部可抢占者仍不足 → 本轮不抢占，避免无意义抖动
	}
	return chosen
}

func pos(x int64) int64 {
	if x < 0 {
		return 0
	}
	return x
}

// dispatch 将作业置为运行并持久化（持锁调用，调用方已保证资源可容纳）。
func (s *Scheduler) dispatch(job *Job, eff int64) {
	start := s.tick
	finish := s.tick + job.Remaining
	if err := s.emit(&Event{Tick: s.tick, Type: EvDispatched, JobID: job.ID, Tenant: job.Tenant,
		StartTick: start, FinishTick: finish, EffPriority: eff, Remaining: job.Remaining}); err != nil {
		s.log.Printf("fatal: persist dispatch failed: %v", err)
		return
	}
	s.log.Printf("调度: job=%s prio=%d eff=%d demand=%s 占用后 used=%s/%s tenantUsed=%s/%s 预计完成tick=%d",
		job.ID, job.Priority, eff, job.Request, s.usedTotal, s.cfg.Capacity,
		s.used[job.Tenant], s.tenants[job.Tenant].Quota, finish)
}

// Snapshot 返回全量状态快照（各列表均排序，保证可比较/可解释）。
func (s *Scheduler) Snapshot() StateSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap := StateSnapshot{Tick: s.tick, Capacity: s.cfg.Capacity, Used: s.usedTotal,
		Tenants: []TenantSnapshot{}, Jobs: []Job{}}

	tenantNames := append([]string{}, s.tenantOrder...)
	sort.Strings(tenantNames)
	for _, name := range tenantNames {
		t := s.tenants[name]
		ts := TenantSnapshot{Name: name, Quota: t.Quota, Used: s.used[name], Running: []string{}, Pending: []string{}}
		for _, j := range s.jobs {
			if j.Tenant != name {
				continue
			}
			switch j.State {
			case StateRunning:
				ts.Running = append(ts.Running, j.ID)
			case StatePending:
				ts.Pending = append(ts.Pending, j.ID)
			}
		}
		sort.Strings(ts.Running)
		sort.Strings(ts.Pending)
		snap.Tenants = append(snap.Tenants, ts)
	}

	ids := make([]string, 0, len(s.jobs))
	for id := range s.jobs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		snap.Jobs = append(snap.Jobs, *s.jobs[id].Clone())
	}
	return snap
}

// CurrentTick 返回当前 tick。
func (s *Scheduler) CurrentTick() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tick
}

// Job 返回指定作业快照，不存在返回 nil。
func (s *Scheduler) Job(id string) *Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.jobs[id].Clone()
}

// Close 关闭底层存储。
func (s *Scheduler) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store.Close()
}
