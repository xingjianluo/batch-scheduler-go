// Package scheduler 的推演（Rehearsal）能力：
// 在完全不影响真实调度器的前提下，从某个状态点出发，按调用方驱动的
// 逻辑时间顺序重放一串事件（提交/完成/取消/推进时间），并把推演进度
// 本身持久化，支持崩溃后续跑。
package scheduler

import (
	"errors"
	"sync"
	"time"
)

// manualClock 是只能通过推演事件显式推进的逻辑时钟：
// 永不读取墙钟，从而保证“同一段事件瞬间跑完与隔几秒分批跑完结论一致”。
type manualClock struct{ t time.Time }

func (c *manualClock) Now() time.Time { return c.t }

// rehearsal 相关错误。
var (
	// ErrRehearsalCheckpointExists 目标路径已有推演存档，避免覆盖另一轮推演。
	ErrRehearsalCheckpointExists = errors.New("rehearsal checkpoint already exists")
	// ErrRehearsalNotACheckpoint 目标文件不是本包写出的推演存档。
	ErrRehearsalNotACheckpoint = errors.New("file is not a rehearsal checkpoint")
	// ErrRehearsalUnsupportedVersion 推演存档版本不被当前代码识别。
	ErrRehearsalUnsupportedVersion = errors.New("unsupported rehearsal checkpoint version")
	// ErrRehearsalTimeGoesBack 推演事件试图把逻辑时间向过去推进。
	ErrRehearsalTimeGoesBack = errors.New("rehearsal logical time must not go backwards")
)

// rehearsalStateVersion 是推演存档的格式版本。
const rehearsalStateVersion = 1

// RehearsalKind 标识事件类型。
type RehearsalKind int

const (
	// RehearsalSubmit 提交新作业（字段同 Scheduler.Submit）。
	RehearsalSubmit RehearsalKind = iota + 1
	// RehearsalComplete 完成指定作业。
	RehearsalComplete
	// RehearsalCancel 取消指定作业。
	RehearsalCancel
	// RehearsalAdvance 把逻辑时间推进到指定时刻。
	RehearsalAdvance
)

// RehearsalEvent 是喂给推演器的一个事件。
// Kind 决定各字段含义：
//   - RehearsalSubmit: JobID/TenantID/Priority/Request
//   - RehearsalComplete/Cancel: JobID
//   - RehearsalAdvance: At
type RehearsalEvent struct {
	Kind     RehearsalKind
	JobID    string
	TenantID string
	Priority int
	Request  Resource
	At       time.Time
}

// RehearsalStepReport 是一步事件的推演结果。
type RehearsalStepReport struct {
	Index     int // 该步在整轮推演中的序号（从 0 开始）
	Event     RehearsalEvent
	Time      time.Time // 本步执行时的逻辑时间
	Decisions []Decision
	// Submit 事件的受理结果；其他事件为 nil。
	Submit *SubmitResult
	// 本步返回的错误：提交拒绝（*RejectError）、操作错误（ErrJobNotFound 等）
	// 或时间回退错误；成功时为 nil。
	Err      error
	Snapshot *Snapshot
}

// Rehearsal 是一轮独立推演：拥有自己的状态副本、逻辑时钟与存档文件，
// 与真实调度器完全隔离。其方法可被多个 goroutine 并发调用（内部加锁）。
type Rehearsal struct {
	mu sync.Mutex

	// 以下字段在构造时确定，运行期不变。
	cfg       Config
	startTime time.Time
	path      string
	saveFunc  func(*rehearsalCheckpoint) error

	sched *Scheduler
	steps []RehearsalStepReport
}

// NewRehearsalFromLive 从真实调度器当前状态创建一轮新推演。
// 仅读取（加锁拷贝）真实调度器状态，之后与它再无任何共享；真实调度器
// 后续继续运行不受影响。checkpointPath 非空时推演进度原子持久化到该文件，
// 进程崩溃后可用 OpenRehearsal 续跑；为空则纯内存。
// 若 checkpointPath 已存在推演存档，返回 ErrRehearsalCheckpointExists，
// 防止无意中覆盖另一轮推演的进度。
func NewRehearsalFromLive(s *Scheduler, checkpointPath string) (*Rehearsal, error) {
	s.mu.Lock()
	st := s.exportStateLocked()
	cfg := s.cfg
	startTime := s.clock.Now()
	s.mu.Unlock()
	return newRehearsal(checkpointPath, cfg, startTime, st, false)
}

// NewRehearsalFromStateFile 从一个已保存的调度器状态文件创建推演。
// 该文件全程只读，绝不被推演改写；推演进度写到 checkpointPath。
// cfg 必须与当初产生该状态文件的调度配置一致（容量/老化参数决定调度结论），
// startTime 为推演起始逻辑时间（通常取保存该状态点时的时间）。
func NewRehearsalFromStateFile(statePath, checkpointPath string, cfg Config, startTime time.Time) (*Rehearsal, error) {
	if configInvalid(cfg) {
		return nil, ErrInvalidArgument
	}
	cfg = normalizeConfig(cfg)
	store, err := newFileStore(statePath)
	if err != nil {
		return nil, err
	}
	st, err := store.Load()
	if err != nil {
		return nil, err
	}
	if st == nil {
		st = &persistedState{
			Tenants:     make(map[string]Tenant),
			Jobs:        make(map[string]*Job),
			QueuedOrder: []string{},
		}
	}
	return newRehearsal(checkpointPath, cfg, startTime, st, true)
}

// OpenRehearsal 从最近一次存档恢复推演，从中断点续跑。
// 续跑使用存档中的容量/老化参数与逻辑时间，最终结论与一口气跑完完全一致。
func OpenRehearsal(checkpointPath string) (*Rehearsal, error) {
	cp, exists, err := loadRehearsalCheckpoint(checkpointPath)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrRehearsalNotACheckpoint
	}
	cfg := Config{
		Capacity:     cp.Capacity,
		AgeIncrement: cp.AgeIncrement,
		AgingPeriod:  time.Duration(cp.AgingPeriod),
	}
	r, err := assembleRehearsal(checkpointPath, cfg, cp.LogicalTime, cp.toPersistedState())
	if err != nil {
		return nil, err
	}
	// 重建历史步骤报告，保证重启后 Steps() 与不中断时逐项一致。
	r.steps = make([]RehearsalStepReport, 0, len(cp.Steps))
	for i, rec := range cp.Steps {
		r.steps = append(r.steps, rec.toReport(i))
	}
	return r, nil
}

// assembleRehearsal 装配一个推演实例（不做存档是否存在的检查、不写初始存档）。
func assembleRehearsal(checkpointPath string, cfg Config, startTime time.Time, st *persistedState) (*Rehearsal, error) {
	clk := &manualClock{t: startTime}
	sched, err := newIsolatedScheduler(cfg, clk, st)
	if err != nil {
		return nil, err
	}
	r := &Rehearsal{
		cfg:       cfg,
		startTime: startTime,
		path:      checkpointPath,
		sched:     sched,
		steps:     []RehearsalStepReport{},
	}
	if checkpointPath != "" {
		r.saveFunc = func(cp *rehearsalCheckpoint) error {
			return saveRehearsalCheckpoint(checkpointPath, cp)
		}
	}
	return r, nil
}

// newRehearsal 是“新建推演”的公共入口：目标路径已有存档（哪怕是普通状态
// 文件）时拒绝创建，避免覆盖；随后原子写入第 0 版存档（种子状态），
// 保证“第一步执行前崩溃”也能正确续跑。
// seedAlreadyLoaded 为 true 时调用方已完成配置归一化与文件加载。
func newRehearsal(checkpointPath string, cfg Config, startTime time.Time, st *persistedState, seedAlreadyLoaded bool) (*Rehearsal, error) {
	if !seedAlreadyLoaded {
		if configInvalid(cfg) {
			return nil, ErrInvalidArgument
		}
		cfg = normalizeConfig(cfg)
	}
	// 目标路径已被任何文件占用时拒绝创建（OpenRehearsal 才是续跑入口）。
	if checkpointPath != "" {
		if _, exists, err := loadRehearsalCheckpoint(checkpointPath); err != nil {
			if errors.Is(err, ErrRehearsalNotACheckpoint) {
				return nil, ErrRehearsalCheckpointExists
			}
			return nil, err
		} else if exists {
			return nil, ErrRehearsalCheckpointExists
		}
	}
	r, err := assembleRehearsal(checkpointPath, cfg, startTime, st)
	if err != nil {
		return nil, err
	}
	if checkpointPath != "" {
		if err := r.persistLocked(); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// Apply 应用单个事件：
//
//	先在当前隔离副本上演算事件；演算成功后原子写入本步存档；
//	只有存档成功，新状态才对调用方生效。
//
// 原子性语义：演算成功但存档失败时，丢弃演算用的候选副本、保留旧状态，
// 对外等同于该步从未发生；调用方随后原样重试同一步不会多占资源、
// 不会让队列/状态错位。纯内存推演（无存档路径）时演算成功即生效。
//
// 业务结果（提交被拒绝、作业不存在、状态不符等）记录在返回报告的 Err 中，
// 属于事件的正常演算结果，会随存档保存；本方法只为这类结果返回 nil error。
// 只有基础设施错误（存档 IO 失败）才以非 nil error 返回。
func (r *Rehearsal) Apply(ev RehearsalEvent) (*RehearsalStepReport, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// 时间回退属于调用方用法错误：既不演算也不存档，状态原地不动，
	// 重试一个合法事件即可继续。
	if ev.Kind == RehearsalAdvance && ev.At.Before(r.sched.clock.Now()) {
		return nil, ErrRehearsalTimeGoesBack
	}

	// 1) 深拷贝当前状态，在“候选副本”上演算；现有副本与其时钟完全不动，
	//    保证存档失败（尤其是 Advance）时没有任何部分效果泄漏。
	cp0 := r.snapshotStateLocked()
	candClock := &manualClock{t: r.sched.clock.Now()}
	cand, err := newIsolatedScheduler(r.cfg, candClock, cp0)
	if err != nil {
		return nil, err
	}
	now := cand.clock.Now()
	rep := &RehearsalStepReport{
		Index: len(r.steps),
		Event: ev,
		Time:  now,
	}

	switch ev.Kind {
	case RehearsalSubmit:
		res, serr := cand.Submit(ev.JobID, ev.TenantID, ev.Priority, ev.Request)
		if res != nil {
			rep.Submit = res
			rep.Decisions = res.Decisions
		}
		rep.Err = serr
	case RehearsalComplete:
		// 记录“完成”本身及其连锁引发的全部调度裁决（启动/抢占/重新排队），
		// 与真跑同一步得到的完整裁决列表逐项一致。
		ds, derr := cand.complete(ev.JobID)
		rep.Decisions = ds
		rep.Err = derr
	case RehearsalCancel:
		// 同上：取消引发的级联调度动作完整入档。
		ds, derr := cand.cancel(ev.JobID)
		rep.Decisions = ds
		rep.Err = derr
	case RehearsalAdvance:
		candClock.t = ev.At
		ds, terr := cand.Tick(ev.At)
		rep.Time = ev.At
		rep.Decisions = ds
		rep.Err = terr
		if terr != nil {
			// Tick 在纯内存副本上不会失败（nopStore），此处防御性回退。
			break
		}
	default:
		return nil, ErrInvalidArgument
	}

	// 2) 演算完成（含业务拒绝/错误），先取本步生效后的状态快照，
	//    再原子持久化本步（快照随报告一起入档，重启后历史步骤仍能看到当时状态）。
	rep.Snapshot = cand.Snapshot()
	if r.path != "" {
		checkpoint := r.buildCheckpointLocked(cand, rep)
		if err := r.saveFunc(checkpoint); err != nil {
			// 丢弃候选副本：对外（含后续重启）看不到这一步的任何痕迹。
			return nil, err
		}
	}

	// 3) 存档成功（或纯内存）：候选副本转正。
	r.sched = cand
	r.steps = append(r.steps, *rep)
	return rep, nil
}

// ApplyAll 顺序应用全部事件。每一步的业务结果（含拒绝与操作错误）记录在
// 对应步骤报告中并继续后续事件；仅当 Apply 返回基础设施错误（存档失败）时
// 中断并返回已成功步骤与该错误。
func (r *Rehearsal) ApplyAll(events []RehearsalEvent) ([]RehearsalStepReport, error) {
	out := make([]RehearsalStepReport, 0, len(events))
	for _, ev := range events {
		rep, err := r.Apply(ev) // Apply 自行加锁
		if err != nil {
			return out, err
		}
		out = append(out, *rep)
	}
	return out, nil
}

// Steps 返回截至目前所有已成功生效步骤的报告副本。
func (r *Rehearsal) Steps() []RehearsalStepReport {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]RehearsalStepReport, len(r.steps))
	copy(out, r.steps)
	return out
}

// Snapshot 返回推演内调度器当前状态的快照（时间为逻辑时间）。
func (r *Rehearsal) Snapshot() *Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sched.Snapshot()
}

// Now 返回推演当前逻辑时间。
func (r *Rehearsal) Now() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sched.clock.Now()
}

// Applied 返回已成功生效（并已存档）的事件步数。
func (r *Rehearsal) Applied() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.steps)
}

// Close 释放推演资源。每一步生效时均已原子存档，Close 不再额外写盘，
// 因而 Close 本身失败也不会使已生效步骤丢失。
func (r *Rehearsal) Close() error { return nil }

// snapshotStateLocked 深拷贝推演当前调度器状态（含资源账目会在重建时重算）。
func (r *Rehearsal) snapshotStateLocked() *persistedState {
	r.sched.mu.Lock()
	defer r.sched.mu.Unlock()
	return r.sched.exportStateLocked()
}

// buildCheckpointLocked 用候选副本的状态与本步报告组装存档。
func (r *Rehearsal) buildCheckpointLocked(cand *Scheduler, rep *RehearsalStepReport) *rehearsalCheckpoint {
	cand.mu.Lock()
	st := cand.exportStateLocked()
	now := cand.clock.Now()
	cand.mu.Unlock()

	recs := make([]stepRecord, 0, len(r.steps)+1)
	for i := range r.steps {
		recs = append(recs, stepRecordFromReport(&r.steps[i]))
	}
	recs = append(recs, stepRecordFromReport(rep))

	return &rehearsalCheckpoint{
		Version:      rehearsalStateVersion,
		Kind:         rehearsalFileKind,
		NextSeq:      st.NextSeq,
		Tenants:      st.Tenants,
		Jobs:         st.Jobs,
		QueuedOrder:  st.QueuedOrder,
		Capacity:     r.cfg.Capacity,
		AgeIncrement: r.cfg.AgeIncrement,
		AgingPeriod:  int64(r.cfg.AgingPeriod),
		LogicalTime:  now,
		Steps:        recs,
	}
}

// persistLocked 写入仅含种子状态、零步骤的初始存档。
func (r *Rehearsal) persistLocked() error {
	r.sched.mu.Lock()
	st := r.sched.exportStateLocked()
	now := r.sched.clock.Now()
	r.sched.mu.Unlock()
	cp := &rehearsalCheckpoint{
		Version:      rehearsalStateVersion,
		Kind:         rehearsalFileKind,
		NextSeq:      st.NextSeq,
		Tenants:      st.Tenants,
		Jobs:         st.Jobs,
		QueuedOrder:  st.QueuedOrder,
		Capacity:     r.cfg.Capacity,
		AgeIncrement: r.cfg.AgeIncrement,
		AgingPeriod:  int64(r.cfg.AgingPeriod),
		LogicalTime:  now,
		Steps:        []stepRecord{},
	}
	if r.saveFunc != nil {
		return r.saveFunc(cp)
	}
	return saveRehearsalCheckpoint(r.path, cp)
}

// toPersistedState 把存档中与调度器同构的部分取出，用于重建隔离调度器。
func (cp *rehearsalCheckpoint) toPersistedState() *persistedState {
	return &persistedState{
		NextSeq:     cp.NextSeq,
		Tenants:     cp.Tenants,
		Jobs:        cp.Jobs,
		QueuedOrder: append([]string{}, cp.QueuedOrder...),
	}
}

// stepRecordFromReport 把步骤报告序列化为存档记录（错误分类保存）。
func stepRecordFromReport(rep *RehearsalStepReport) stepRecord {
	rec := stepRecord{
		Event:     rep.Event,
		Time:      rep.Time,
		Decisions: append([]Decision{}, rep.Decisions...),
		Snapshot:  rep.Snapshot,
	}
	// 仅当提交被受理时才记录受理结果；被拒绝的提交此处全部留空，
	// 重启后据此还原“受理/拒绝”的原始结论（拒绝原因在 Err 字段）。
	if rep.Event.Kind == RehearsalSubmit && rep.Submit != nil {
		rec.SubmitJobID = rep.Event.JobID
		rec.Started = rep.Submit.Started
		if rep.Submit.Job != nil {
			j := *rep.Submit.Job
			rec.SubmitJob = &j
		}
	}
	if rep.Err != nil {
		rec.HasError = true
		rec.ErrText = rep.Err.Error()
		if rj, ok := rep.Err.(*RejectError); ok {
			rec.ErrIsReject = true
			rec.ErrKind = rj.Kind
			// 原始 Reason 文本也保留，重启后错误字符串逐项一致。
			rec.ErrText = rj.Reason
			return rec
		}
		switch {
		case errors.Is(rep.Err, ErrJobNotFound):
			rec.ErrSentinel = "ErrJobNotFound"
		case errors.Is(rep.Err, ErrJobNotRunning):
			rec.ErrSentinel = "ErrJobNotRunning"
		case errors.Is(rep.Err, ErrJobAlreadyTerminal):
			rec.ErrSentinel = "ErrJobAlreadyTerminal"
		case errors.Is(rep.Err, ErrInvalidArgument):
			rec.ErrSentinel = "ErrInvalidArgument"
		case errors.Is(rep.Err, ErrRehearsalTimeGoesBack):
			rec.ErrSentinel = "ErrRehearsalTimeGoesBack"
		}
	}
	return rec
}

// toReport 把存档记录重建为步骤报告（错误按类别还原为等值错误）。
// 历史步骤的受理结果、当时状态快照与完整裁决列表全部还原，
// 保证重启续跑后 Steps() 与不中断连续跑完逐项一致。
func (rec stepRecord) toReport(index int) RehearsalStepReport {
	rep := RehearsalStepReport{
		Index:     index,
		Event:     rec.Event,
		Time:      rec.Time,
		Decisions: append([]Decision{}, rec.Decisions...),
		Snapshot:  rec.Snapshot,
	}
	if rec.SubmitJobID != "" {
		sub := &SubmitResult{
			Started:   rec.Started,
			Decisions: rep.Decisions,
		}
		if rec.SubmitJob != nil {
			j := *rec.SubmitJob
			sub.Job = &j
		}
		if len(rep.Decisions) > 0 {
			sub.Decision = rep.Decisions[0] // 受理裁决（admit-queued）
		}
		rep.Submit = sub
	}
	if rec.HasError {
		switch rec.ErrSentinel {
		case "ErrJobNotFound":
			rep.Err = ErrJobNotFound
		case "ErrJobNotRunning":
			rep.Err = ErrJobNotRunning
		case "ErrJobAlreadyTerminal":
			rep.Err = ErrJobAlreadyTerminal
		case "ErrInvalidArgument":
			rep.Err = ErrInvalidArgument
		case "ErrRehearsalTimeGoesBack":
			rep.Err = ErrRehearsalTimeGoesBack
		default:
			if rec.ErrIsReject {
				// 还原为与原始拒绝等值的错误：Kind 一致即可编程分类，
				// Reason 保存的是原始 newReject 格式化文本，Error() 字符串亦一致。
				rep.Err = &RejectError{Kind: rec.ErrKind, Reason: rec.ErrText}
			} else {
				rep.Err = errors.New(rec.ErrText)
			}
		}
	}
	return rep
}

// 便捷构造事件的函数。

// NewSubmitEvent 构造提交事件。
func NewSubmitEvent(jobID, tenantID string, priority int, req Resource) RehearsalEvent {
	return RehearsalEvent{Kind: RehearsalSubmit, JobID: jobID, TenantID: tenantID, Priority: priority, Request: req}
}

// NewCompleteEvent 构造完成事件。
func NewCompleteEvent(jobID string) RehearsalEvent {
	return RehearsalEvent{Kind: RehearsalComplete, JobID: jobID}
}

// NewCancelEvent 构造取消事件。
func NewCancelEvent(jobID string) RehearsalEvent {
	return RehearsalEvent{Kind: RehearsalCancel, JobID: jobID}
}

// NewAdvanceEvent 构造逻辑时间推进事件。
func NewAdvanceEvent(at time.Time) RehearsalEvent {
	return RehearsalEvent{Kind: RehearsalAdvance, At: at}
}
