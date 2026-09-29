package scheduler

import (
	"errors"
	"sort"
	"sync"
	"time"
)

// 默认老化参数：每等待 AgingPeriod，等效优先级提升 AgeIncrement。
// 老化保证排队作业的等效优先级单调增长，最终必然超过运行中作业的固定
// 基础优先级，从而可以抢占获得资源，避免无限期饿死。
const (
	defaultAgeIncrement = 1
	defaultAgingPeriod  = time.Second
)

// 常见操作错误（与提交拒绝错误 RejectError 区分）。
var (
	ErrJobNotFound        = errors.New("job not found")
	ErrJobNotRunning      = errors.New("job is not running")
	ErrJobAlreadyTerminal = errors.New("job already in terminal state")
	ErrInvalidArgument    = errors.New("invalid argument")
)

// systemClock 使用系统墙钟。
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// Scheduler 是本地批处理作业调度器。
// 所有可变状态均由 mu 保护；每次状态变更后原子持久化（若配置）。
type Scheduler struct {
	cfg   Config
	clock Clock
	store Store
	mu    sync.Mutex
	pool  *pool

	nextSeq int64
	tenants map[string]Tenant
	jobs    map[string]*Job
	// queuedIDs 始终按 SubmitSeq 升序保存（被抢占作业按原序号回到原位）。
	queuedIDs []string
}

// New 创建调度器并注册初始租户；若配置了持久化路径则恢复历史状态。
func New(cfg Config, tenants []Tenant) (*Scheduler, error) {
	store, err := newFileStore(cfg.StorePath)
	if err != nil {
		return nil, err
	}
	return newWithStore(cfg, tenants, store)
}

// newWithStore 与 New 相同，但使用调用方给定的存储实现
// （推演器借此把内部调度器接到内存存储上，复用同一套调度与恢复逻辑）。
func newWithStore(cfg Config, tenants []Tenant, store Store) (*Scheduler, error) {
	clock := cfg.Clock
	if clock == nil {
		clock = systemClock{}
	}
	if cfg.AgeIncrement < 0 || cfg.AgingPeriod < 0 {
		return nil, ErrInvalidArgument
	}
	if cfg.AgingPeriod == 0 {
		cfg.AgingPeriod = defaultAgingPeriod
	}
	if cfg.AgeIncrement == 0 {
		cfg.AgeIncrement = defaultAgeIncrement
	}
	s := &Scheduler{
		cfg:     cfg,
		clock:   clock,
		store:   store,
		pool:    newPool(cfg.Capacity),
		nextSeq: 1,
		tenants: make(map[string]Tenant),
		jobs:    make(map[string]*Job),
	}
	for _, t := range tenants {
		if err := s.validateTenant(t); err != nil {
			return nil, err
		}
		s.tenants[t.ID] = t
	}
	if err := s.restore(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Scheduler) validateTenant(t Tenant) error {
	if t.ID == "" {
		return errors.Join(ErrInvalidArgument, errors.New("tenant id is empty"))
	}
	if t.Quota.CPU < 0 || t.Quota.Memory < 0 || t.Quota.GPU < 0 {
		return errors.Join(ErrInvalidArgument, errors.New("tenant quota contains negative value"))
	}
	// 注意：租户配额允许大于集群物理容量——配额是租户瞬时持有上限，
	// 集群容量是共享物理上限，二者独立约束（这样两类拒绝原因可区分）。
	if t.MaxConcurrent < 0 {
		return errors.Join(ErrInvalidArgument, errors.New("max concurrent is negative"))
	}
	return nil
}

// restore 从持久化状态重建内存结构与资源记账。
// 崩溃恢复语义：Running 作业仍视为运行中并继续占位；排队作业保持排队；
// Completed/Canceled 终态作业保留记录但永远不会被再次调度。
func (s *Scheduler) restore() error {
	st, err := s.store.Load()
	if err != nil {
		return err
	}
	if st == nil {
		// 首次启动：持久化初始租户集合。
		return s.persistLocked()
	}
	s.nextSeq = st.NextSeq
	if st.Tenants != nil {
		s.tenants = st.Tenants
	}
	s.jobs = st.Jobs
	s.queuedIDs = st.QueuedOrder
	if s.queuedIDs == nil {
		s.queuedIDs = []string{}
	}
	s.pool = newPool(s.cfg.Capacity)
	for _, id := range s.runningIDsLocked() {
		j := s.jobs[id]
		s.pool.allocate(j.TenantID, j.Request)
	}
	return nil
}

// RegisterTenant 注册或更新一个租户配额（更新配额不影响已运行作业）。
func (s *Scheduler) RegisterTenant(t Tenant) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateTenant(t); err != nil {
		return err
	}
	s.tenants[t.ID] = t
	return s.persistLocked()
}

// SubmitResult 是提交的返回结果。
type SubmitResult struct {
	Job      *Job
	Started  bool
	Decision Decision
	// Decisions 包含本次提交引发的全部裁决（受理 + 后续启动/抢占），
	// Decisions[0] 即 Decision，便于完整解释调度结论。
	Decisions []Decision
}

// Submit 提交作业。
// 拒绝原因（按校验顺序，彼此可区分）：
//  1. InvalidRequest：ID/租户ID 为空或资源需求存在负值；
//  2. TenantNotFound：租户未注册；
//  3. DuplicateJob：作业 ID 已存在（含终态，终态 ID 不可复用）；
//  4. ExceedsClusterCapacity：需求超过集群物理总容量（永远无法满足）；
//  5. ExceedsTenantQuota：需求超过租户配额上限（即使全空也无法满足）。
func (s *Scheduler) Submit(id, tenantID string, priority int, req Resource) (*SubmitResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if id == "" {
		return nil, newReject(RejectInvalidRequest, "job id is empty")
	}
	if req.CPU < 0 || req.Memory < 0 || req.GPU < 0 {
		return nil, newReject(RejectInvalidRequest, "resource request contains negative value: %+v", req)
	}
	if tenantID == "" {
		return nil, newReject(RejectInvalidRequest, "tenant id is empty")
	}
	t, ok := s.tenants[tenantID]
	if !ok {
		return nil, newReject(RejectTenantNotFound, "tenant %q not registered", tenantID)
	}
	if _, dup := s.jobs[id]; dup {
		return nil, newReject(RejectDuplicateJob, "job %q already submitted", id)
	}
	if !s.cfg.Capacity.Fits(req) {
		return nil, newReject(RejectExceedsClusterCapacity,
			"request %+v exceeds cluster capacity %+v", req, s.cfg.Capacity)
	}
	if !t.Quota.Fits(req) {
		return nil, newReject(RejectExceedsTenantQuota,
			"request %+v exceeds tenant %q quota %+v (can never fit)", req, tenantID, t.Quota)
	}
	// 注意：仅当需求“原则上”超过静态配额上限才拒绝。当前配额被其他运行中
	// 作业占用属于暂时状态（作业完成后即可释放），因此新作业正常受理排队，
	// 是否真正启动由 schedule 阶段的容量+配额双重检查决定，杜绝超卖。

	now := s.clock.Now()
	seq := s.nextSeq
	s.nextSeq++
	job := &Job{
		ID:          id,
		TenantID:    tenantID,
		Priority:    priority,
		Request:     req,
		SubmitSeq:   seq,
		SubmittedAt: now,
		State:       StateQueued,
		QueuedAt:    now,
	}
	s.jobs[id] = job
	s.insertQueuedLocked(job)

	admit := Decision{
		Time:     now,
		JobID:    id,
		Action:   "admit-queued",
		Reason:   "job accepted and enqueued",
		JobState: StateQueued,
		Used:     s.pool.usage(),
		Capacity: s.cfg.Capacity,
	}
	decisions := []Decision{admit}
	decisions = append(decisions, s.runScheduleLocked(now)...)
	if err := s.persistLocked(); err != nil {
		return nil, err
	}
	started := s.jobs[id].State == StateRunning
	return &SubmitResult{
		Job:       cloneJob(job),
		Started:   started,
		Decision:  admit,
		Decisions: decisions,
	}, nil
}

// Complete 将运行中作业标记为完成，释放其全部资源占位并触发调度。
func (s *Scheduler) Complete(jobID string) (*Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	decisions, err := s.completeLocked(jobID)
	if err != nil {
		return nil, err
	}
	if err := s.persistLocked(); err != nil {
		return nil, err
	}
	return &decisions[0], nil
}

// completeLocked 是 Complete 的持锁实现，返回本步全部裁决（含后续调度级联），
// 供推演器记录完整的判断依据。
func (s *Scheduler) completeLocked(jobID string) ([]Decision, error) {
	job, ok := s.jobs[jobID]
	if !ok {
		return nil, ErrJobNotFound
	}
	if job.State == StateCompleted || job.State == StateCanceled {
		return nil, ErrJobAlreadyTerminal
	}
	if job.State != StateRunning {
		return nil, ErrJobNotRunning
	}
	now := s.clock.Now()
	s.pool.release(job.TenantID, job.Request)
	job.State = StateCompleted
	job.FinishedAt = now
	d := Decision{
		Time: now, JobID: jobID, Action: "complete",
		Reason: "job finished and resources released", JobState: StateCompleted,
		Used: s.pool.usage(), Capacity: s.cfg.Capacity,
	}
	decisions := []Decision{d}
	decisions = append(decisions, s.runScheduleLocked(now)...)
	return decisions, nil
}

// Cancel 取消作业：排队中则直接离开队列；运行中则释放资源。终态不可取消。
func (s *Scheduler) Cancel(jobID string) (*Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	decisions, err := s.cancelLocked(jobID)
	if err != nil {
		return nil, err
	}
	if err := s.persistLocked(); err != nil {
		return nil, err
	}
	return &decisions[0], nil
}

// cancelLocked 是 Cancel 的持锁实现，返回本步全部裁决（含后续调度级联）。
func (s *Scheduler) cancelLocked(jobID string) ([]Decision, error) {
	job, ok := s.jobs[jobID]
	if !ok {
		return nil, ErrJobNotFound
	}
	if job.State == StateCompleted || job.State == StateCanceled {
		return nil, ErrJobAlreadyTerminal
	}
	now := s.clock.Now()
	if job.State == StateRunning {
		s.pool.release(job.TenantID, job.Request)
	}
	if job.State == StateQueued {
		s.removeQueuedLocked(jobID)
	}
	job.State = StateCanceled
	job.FinishedAt = now
	d := Decision{
		Time: now, JobID: jobID, Action: "cancel",
		Reason: "job canceled by client", JobState: StateCanceled,
		Used: s.pool.usage(), Capacity: s.cfg.Capacity,
	}
	decisions := []Decision{d}
	decisions = append(decisions, s.runScheduleLocked(now)...)
	return decisions, nil
}

// Get 查询作业当前状态副本。
func (s *Scheduler) Get(jobID string) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[jobID]
	if !ok {
		return nil, ErrJobNotFound
	}
	return cloneJob(job), nil
}

// Tick 在指定时刻执行一轮调度（测试可用假时钟确定性驱动；
// 正常使用中 Submit/Complete/Cancel 已会自动触发调度）。
// 非抢占启动持续级联直到固定点；发生一次抢占或无进展即结束。
// 返回本轮全部裁决；持久化失败时错误向上透传（内存裁决仍然有效）。
func (s *Scheduler) Tick(now time.Time) ([]Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	all := s.runScheduleLocked(now)
	if err := s.persistLocked(); err != nil {
		return all, err
	}
	return all, nil
}

// effectivePriority 计算作业在 now 时刻的等效优先级。
// 排队作业：基础优先级 + 老化加成（按自 QueuedAt 起等待的整周期数）。
// 运行中作业：仅基础优先级（运行即获得进展，不参与老化）。
func effectivePriority(j *Job, now time.Time, inc int, period time.Duration) int {
	if j.State == StateRunning {
		return j.Priority
	}
	waited := now.Sub(j.QueuedAt)
	if waited < 0 || period <= 0 || inc == 0 {
		return j.Priority
	}
	return j.Priority + int(waited/period)*inc
}

// schedule 执行单趟调度：队列按 (等效优先级降序, SubmitSeq 升序) 遍历，
// 能直接准入则启动（非抢占启动可在一轮内级联填满资源）；否则尝试抢占
// 严格更低等效优先级的运行中作业。相同等效优先级之间不发生抢占。
//
// 抢占是昂贵且具有破坏性的决策：一轮调度最多解决一次抢占，随后立即结束
// 本轮（返回 stopRound=true）。被抢占作业按原 SubmitSeq 重新排队、等待
// 年龄重新累积，留待下一轮事件（提交/完成/取消/Tick）再议。
// 这避免了“抢占者与被抢占者在同一轮互相反超”的抖动；跨轮次看，每个作业
// 都会周期性获得运行窗口，因而不存在无限期饿死。
func (s *Scheduler) schedule(now time.Time) (decisions []Decision, stopRound bool) {
	order := make([]*Job, 0, len(s.queuedIDs))
	for _, id := range s.queuedIDs {
		order = append(order, s.jobs[id])
	}
	sort.SliceStable(order, func(a, b int) bool {
		pa := effectivePriority(order[a], now, s.cfg.AgeIncrement, s.cfg.AgingPeriod)
		pb := effectivePriority(order[b], now, s.cfg.AgeIncrement, s.cfg.AgingPeriod)
		if pa != pb {
			return pa > pb
		}
		return order[a].SubmitSeq < order[b].SubmitSeq
	})

	// 严格队首（Head-of-Line）规则：严格按上述顺序逐个考察，
	// 一旦遇到当前既无法直接启动、抢占后也无法满足的作业，立即结束本趟。
	// 排在其后的作业即便能放入资源空洞也不得插队（无回填），
	// 使调度结论严格确定、易解释；待下一轮事件释放资源后从头重新考察。
	for _, q := range order {
		if q.State != StateQueued {
			continue // 已在本趟更早位置被启动
		}
		quota := s.tenants[q.TenantID].Quota
		maxConc := s.tenants[q.TenantID].MaxConcurrent
		if s.canStartLocked(q.TenantID, quota, maxConc, q.Request) {
			s.startLocked(q, now, nil, &decisions)
			continue
		}
		victims := s.chooseVictimsLocked(q, now, quota, maxConc)
		if len(victims) > 0 {
			s.startLocked(q, now, victims, &decisions)
			return decisions, true // 本轮抢占到此为止
		}
		return decisions, false // 队首阻塞：本趟结束
	}
	return decisions, false
}

// runScheduleLocked 驱动调度直到固定点：非抢占启动持续级联，
// 一旦某趟发生抢占或不再有任何进展即结束。
func (s *Scheduler) runScheduleLocked(now time.Time) []Decision {
	var all []Decision
	for {
		ds, stop := s.schedule(now)
		all = append(all, ds...)
		if stop || len(ds) == 0 {
			break
		}
	}
	return all
}

// canStartLocked 判断当前是否可直接启动（集群容量、租户配额、并发数三重约束）。
func (s *Scheduler) canStartLocked(tenantID string, quota Resource, maxConcurrent int, req Resource) bool {
	if maxConcurrent > 0 && s.tenantRunningCountLocked(tenantID) >= maxConcurrent {
		return false
	}
	return s.pool.canAllocateFor(tenantID, quota, req)
}

// chooseVictimsLocked 按确定性规则挑选最小抢占集合：
// 仅考虑等效优先级严格更低的运行中作业，依次按
// (等效优先级升序, 启动时间降序[后启动先被抢占], 作业ID 升序) 选取，
// 累计回收直到同时满足集群容量、租户配额与并发槽位；
// 即使抢占全部候选仍不足则返回 nil（本轮不抢占）。
func (s *Scheduler) chooseVictimsLocked(q *Job, now time.Time, quota Resource, maxConcurrent int) []*Job {
	qPri := effectivePriority(q, now, s.cfg.AgeIncrement, s.cfg.AgingPeriod)
	var cands []*Job
	for _, j := range s.jobs {
		if j.State != StateRunning {
			continue
		}
		if effectivePriority(j, now, s.cfg.AgeIncrement, s.cfg.AgingPeriod) >= qPri {
			continue
		}
		cands = append(cands, j)
	}
	// 排序键（全部确定、无随机）：
	//  1. 等效优先级升序：先牺牲最弱作业；
	//  2. 跨租户优先（公平性平局规则）：同等条件下优先回收其他租户的占用，
	//     而不是在同一租户内部连环抢占（抢占同租户同样能释放配额/槽位，
	//     但会让该租户的作业互相挤占）；
	//  3. 资源需求降序（GPU→Memory→CPU 字典序）：优先牺牲“大”作业，
	//     用尽量少的牺牲品凑齐稀缺维度（最小抢占集合启发式）；
	//  4. 启动时间降序：后启动者先被牺牲；
	//  5. 作业 ID 升序：最终平局兜底。
	sort.SliceStable(cands, func(a, b int) bool {
		pa := effectivePriority(cands[a], now, s.cfg.AgeIncrement, s.cfg.AgingPeriod)
		pb := effectivePriority(cands[b], now, s.cfg.AgeIncrement, s.cfg.AgingPeriod)
		if pa != pb {
			return pa < pb
		}
		ca := cands[a].TenantID != q.TenantID
		cb := cands[b].TenantID != q.TenantID
		if ca != cb {
			return ca
		}
		ra, rb := cands[a].Request, cands[b].Request
		if ra.GPU != rb.GPU {
			return ra.GPU > rb.GPU
		}
		if ra.Memory != rb.Memory {
			return ra.Memory > rb.Memory
		}
		if ra.CPU != rb.CPU {
			return ra.CPU > rb.CPU
		}
		if !cands[a].StartedAt.Equal(cands[b].StartedAt) {
			return cands[a].StartedAt.After(cands[b].StartedAt)
		}
		return cands[a].ID < cands[b].ID
	})

	// 用影子资源池模拟回收过程，绝不修改真实记账。
	// shadowUsed = 当前真实用量减去已选牺牲品的需求；影子池逐次释放，
	// 天然保证集群维度与租户维度的同一笔资源只被计数一次（避免双算）。
	shadow := newPool(s.cfg.Capacity)
	shadow.used = s.pool.usage()
	for tid, u := range s.pool.tenantUsed {
		shadow.tenantUsed[tid] = u
	}
	tenantRunning := s.tenantRunningCountLocked(q.TenantID)
	var picked []*Job
	for _, v := range cands {
		if shadow.canAllocateFor(q.TenantID, quota, q.Request) &&
			(maxConcurrent == 0 || tenantRunning < maxConcurrent) {
			break
		}
		shadow.release(v.TenantID, v.Request)
		if v.TenantID == q.TenantID {
			tenantRunning--
		}
		picked = append(picked, v)
	}
	if !shadow.canAllocateFor(q.TenantID, quota, q.Request) ||
		(maxConcurrent > 0 && tenantRunning >= maxConcurrent) {
		// 即使抢占全部候选仍不满足，本轮不抢占。
		return nil
	}
	return picked
}

// startLocked 抢占给定牺牲品（如有）并启动作业 q，追加裁决日志。
func (s *Scheduler) startLocked(q *Job, now time.Time, victims []*Job, decisions *[]Decision) {
	var victimIDs []string
	for _, v := range victims {
		s.pool.release(v.TenantID, v.Request)
		v.State = StateQueued
		// 重新入队语义（确定性 + 防饿死 + 防抖动）：
		//  - SubmitSeq 保持不变：按原提交顺序回到 FIFO 位置；
		//  - QueuedAt 重置为当前时刻：该作业已获得过运行进展，
		//    年龄重新累积，避免其立刻反超抢占者造成来回抢占；
		//    老化仍保证其在有界等待后可再次抢占（不会无限期饿死）。
		v.QueuedAt = now
		v.PreemptedTimes++
		s.insertQueuedLocked(v)
		victimIDs = append(victimIDs, v.ID)
		*decisions = append(*decisions, Decision{
			Time: now, JobID: v.ID, Action: "preempt",
			Reason:   "preempted by higher effective priority job",
			JobState: StateQueued, Used: s.pool.usage(),
			Capacity: s.cfg.Capacity,
		})
	}
	s.removeQueuedLocked(q.ID)
	s.pool.allocate(q.TenantID, q.Request)
	q.State = StateRunning
	q.StartedAt = now
	reason := "resources available"
	if len(victims) > 0 {
		reason = "started after preemption"
	}
	*decisions = append(*decisions, Decision{
		Time: now, JobID: q.ID, Action: "start", Reason: reason,
		JobState: StateRunning, Used: s.pool.usage(),
		Capacity: s.cfg.Capacity, VictimIDs: victimIDs,
	})
}

func (s *Scheduler) tenantRunningCountLocked(tenantID string) int {
	n := 0
	for _, j := range s.jobs {
		if j.TenantID == tenantID && j.State == StateRunning {
			n++
		}
	}
	return n
}

func (s *Scheduler) runningIDsLocked() []string {
	var ids []string
	for _, j := range s.jobs {
		if j.State == StateRunning {
			ids = append(ids, j.ID)
		}
	}
	sort.Strings(ids)
	return ids
}

// insertQueuedLocked 按 SubmitSeq 升序把作业插入队列（稳定 FIFO，
// 被抢占作业以其原始 SubmitSeq 回到确定性位置）。
func (s *Scheduler) insertQueuedLocked(j *Job) {
	idx := sort.Search(len(s.queuedIDs), func(i int) bool {
		return s.jobs[s.queuedIDs[i]].SubmitSeq >= j.SubmitSeq
	})
	s.queuedIDs = append(s.queuedIDs, "")
	copy(s.queuedIDs[idx+1:], s.queuedIDs[idx:])
	s.queuedIDs[idx] = j.ID
}

func (s *Scheduler) removeQueuedLocked(id string) {
	for i, qid := range s.queuedIDs {
		if qid == id {
			s.queuedIDs = append(s.queuedIDs[:i], s.queuedIDs[i+1:]...)
			return
		}
	}
}

// Snapshot 返回当前完整状态快照（各列表均为确定性顺序）。
func (s *Scheduler) Snapshot() *Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	jobs := make([]Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, *cloneJob(j))
	}
	sort.SliceStable(jobs, func(a, b int) bool { return jobs[a].SubmitSeq < jobs[b].SubmitSeq })
	tenants := make([]Tenant, 0, len(s.tenants))
	tenantUsed := make(map[string]Resource, len(s.tenants))
	for id, t := range s.tenants {
		tenants = append(tenants, t)
		tenantUsed[id] = s.pool.tenantUsage(id)
	}
	sort.SliceStable(tenants, func(a, b int) bool { return tenants[a].ID < tenants[b].ID })
	return &Snapshot{
		Time:       now,
		Capacity:   s.cfg.Capacity,
		Used:       s.pool.usage(),
		Jobs:       jobs,
		QueuedIDs:  append([]string{}, s.queuedIDs...),
		RunningIDs: s.runningIDsLocked(),
		Tenants:    tenants,
		TenantUsed: tenantUsed,
	}
}

// Close 持久化最新状态并释放底层资源。
func (s *Scheduler) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.persistLocked(); err != nil {
		return err
	}
	return s.store.Close()
}

func (s *Scheduler) persistLocked() error {
	st := &persistedState{
		NextSeq:     s.nextSeq,
		Tenants:     s.tenants,
		Jobs:        s.jobs,
		QueuedOrder: append([]string{}, s.queuedIDs...),
	}
	return s.store.Save(st)
}

// exportStateLocked 导出当前完整状态的深拷贝（供推演器做检查点与回滚）。
func (s *Scheduler) exportStateLocked() *persistedState {
	return clonePersistedState(&persistedState{
		NextSeq:     s.nextSeq,
		Tenants:     s.tenants,
		Jobs:        s.jobs,
		QueuedOrder: append([]string{}, s.queuedIDs...),
	})
}

func cloneJob(j *Job) *Job {
	c := *j
	return &c
}
