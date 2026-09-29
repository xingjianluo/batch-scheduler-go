package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// 本文件实现“先推演再决定”的调度推演能力：
// 从运行中调度器或已保存的状态点出发，按顺序喂入事件（提交/完成/取消/推进时间），
// 在独立副本上重放，完全不打扰原调度器；推演进度可持久化、可中断续跑。
//
// 核心保证：
//  1. 推演与真实运行逐项一致——推演内部直接使用同一套 Scheduler 实现，
//     仅替换时钟（逻辑时钟）与存储（内存存储 + 独立检查点），不存在第二份调度逻辑。
//  2. 所有与时间相关的判断（等待、老化）只依赖调用方推进的逻辑时间，
//     与真实流逝的时间无关：同一段事件无论瞬间跑完还是分多次跑，结论一致。
//  3. 每一步要么彻底生效（检查点原子落盘），要么对外就当没发生过（内存回滚），
//     失败后原样重试不会多占资源、不会错位。

// 推演器哨兵错误。
var (
	// ErrSimCheckpointExists 目标 WorkDir 已存在检查点（应使用 ResumeSimulator 续跑）。
	ErrSimCheckpointExists = errors.New("simulator checkpoint already exists")
	// ErrSimNoCheckpoint 目标 WorkDir 没有可恢复的检查点。
	ErrSimNoCheckpoint = errors.New("simulator checkpoint not found")
	// ErrSimClosed 推演器已关闭。
	ErrSimClosed = errors.New("simulator is closed")
	// ErrSimBackwardTime 逻辑时间不允许倒退。
	ErrSimBackwardTime = errors.New("cannot advance logical time backwards")
)

// defaultSimStart 是未指定 StartTime 时的确定性逻辑起点。
var defaultSimStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// EventType 标识推演事件类型。
type EventType int

const (
	// EventSubmit 新作业进入。
	EventSubmit EventType = iota
	// EventComplete 作业结束。
	EventComplete
	// EventCancel 取消作业。
	EventCancel
	// EventAdvance 把逻辑时间推进到指定时刻（并触发一轮调度）。
	EventAdvance
)

// Event 是一条推演事件；按类型取用相应字段。
type Event struct {
	Type EventType
	// Submit 事件字段：
	JobID    string
	TenantID string
	Priority int
	Request  Resource
	// Advance 事件字段：目标逻辑时刻（必须不早于当前逻辑时间）。
	Time time.Time
}

// SubmitEvent 构造提交事件。
func SubmitEvent(id, tenantID string, priority int, req Resource) Event {
	return Event{Type: EventSubmit, JobID: id, TenantID: tenantID, Priority: priority, Request: req}
}

// CompleteEvent 构造完成事件。
func CompleteEvent(id string) Event { return Event{Type: EventComplete, JobID: id} }

// CancelEvent 构造取消事件。
func CancelEvent(id string) Event { return Event{Type: EventCancel, JobID: id} }

// AdvanceEvent 构造时间推进事件。
func AdvanceEvent(t time.Time) Event { return Event{Type: EventAdvance, Time: t} }

// StepRecord 记录一步推演的完整结果（判断依据 + 步后状态）。
type StepRecord struct {
	Index      int        // 事件序号（从 0 开始）
	Event      Event      // 本步事件
	Time       time.Time  // 本步执行时的逻辑时间
	RejectKind string     // 提交被拒时的类别名（RejectKindName），否则为空
	Err        string     // 其他操作错误（如作业不存在），为空表示无错误
	Decisions  []Decision // 本步引发的全部调度裁决（含抢占级联）
	Snapshot   *Snapshot  // 本步生效后的完整状态
}

// SimConfig 是推演器配置。
type SimConfig struct {
	Capacity     Resource
	AgeIncrement int
	AgingPeriod  time.Duration
	// WorkDir 推演检查点目录：每步生效后在此原子落盘，进程被杀后可续跑。
	// 为空表示纯内存推演（不可续跑）。
	WorkDir string
	// StartTime 逻辑时钟起点；零值使用默认固定起点（确定性）。
	StartTime time.Time

	store simCheckpointStore // 测试注入用检查点存储；nil 时按 WorkDir 创建
}

// Simulator 是调度推演器。Apply 线程安全。
type Simulator struct {
	cfg     SimConfig
	tenants []Tenant
	store   simCheckpointStore

	mu      sync.Mutex
	clock   *manualClock
	sched   *Scheduler
	state   *persistedState // 最近一次已提交（落盘）的状态，作为回滚点
	nextIdx int
	records []StepRecord
	closed  bool
}

// manualClock 是仅由调用方推进的逻辑时钟。
type manualClock struct{ t time.Time }

func (c *manualClock) Now() time.Time { return c.t }

// NewSimulator 创建推演器。
// seedStatePath 非空时从该已保存的调度器状态文件出发（只读，绝不回写）；
// 为空时从空集群出发。若 WorkDir 已存在检查点则返回 ErrSimCheckpointExists。
func NewSimulator(cfg SimConfig, tenants []Tenant, seedStatePath string) (*Simulator, error) {
	store, err := simStoreFor(cfg)
	if err != nil {
		return nil, err
	}
	existing, err := store.Load()
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, ErrSimCheckpointExists
	}
	var st *persistedState
	if seedStatePath != "" {
		fs, err := newFileStore(seedStatePath)
		if err != nil {
			return nil, err
		}
		st, err = fs.Load()
		if err != nil {
			return nil, err
		}
		if st == nil {
			return nil, fmt.Errorf("seed state %q not found or empty", seedStatePath)
		}
		st = clonePersistedState(st)
	}
	m, err := newSimulatorWithState(cfg, tenants, st, store)
	if err != nil {
		return nil, err
	}
	// 初始提交：让续跑方即使不应用任何事件也能恢复到起点。
	if err := m.commitLocked(); err != nil {
		return nil, err
	}
	return m, nil
}

// NewSimulatorFromScheduler 从运行中调度器的当前状态创建推演器。
// 仅在持锁下读取并深拷贝原调度器状态，不打扰其后续运行。
func NewSimulatorFromScheduler(s *Scheduler, cfg SimConfig) (*Simulator, error) {
	store, err := simStoreFor(cfg)
	if err != nil {
		return nil, err
	}
	existing, err := store.Load()
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, ErrSimCheckpointExists
	}
	s.mu.Lock()
	st := s.exportStateLocked()
	s.mu.Unlock()
	m, err := newSimulatorWithState(cfg, nil, st, store)
	if err != nil {
		return nil, err
	}
	// 初始提交：让续跑方即使不应用任何事件也能恢复到起点。
	if err := m.commitLocked(); err != nil {
		return nil, err
	}
	return m, nil
}

// ResumeSimulator 从 WorkDir 中最近一次落盘的检查点恢复推演器继续跑。
// 续跑的最终结论与一口气跑完不中断时完全一致。
func ResumeSimulator(cfg SimConfig) (*Simulator, error) {
	store, err := simStoreFor(cfg)
	if err != nil {
		return nil, err
	}
	cp, err := store.Load()
	if err != nil {
		return nil, err
	}
	if cp == nil {
		return nil, ErrSimNoCheckpoint
	}
	m, err := newSimulatorWithState(cfg, nil, clonePersistedState(cp.State), store)
	if err != nil {
		return nil, err
	}
	m.clock.t = cp.Time
	m.records = append([]StepRecord{}, cp.Records...)
	m.nextIdx = cp.NextIdx
	if err := m.rebuild(); err != nil {
		return nil, err
	}
	return m, nil
}

// newSimulatorWithState 用给定初始状态构建推演器（不触碰检查点，
// 首个检查点由创建路径显式写入，续跑路径则直接复用已存在的检查点）。
func newSimulatorWithState(cfg SimConfig, tenants []Tenant, st *persistedState, store simCheckpointStore) (*Simulator, error) {
	start := cfg.StartTime
	if start.IsZero() {
		start = defaultSimStart
	}
	m := &Simulator{
		cfg:     cfg,
		tenants: tenants,
		store:   store,
		state:   clonePersistedState(st),
	}
	m.clock = &manualClock{t: start}
	if err := m.rebuild(); err != nil {
		return nil, err
	}
	return m, nil
}

// rebuild 从 m.state 重建内部调度器与逻辑时钟（构造与回滚共用）。
func (m *Simulator) rebuild() error {
	mem := &memStore{st: clonePersistedState(m.state)}
	sched, err := newWithStore(Config{
		Capacity:     m.cfg.Capacity,
		AgeIncrement: m.cfg.AgeIncrement,
		AgingPeriod:  m.cfg.AgingPeriod,
		Clock:        m.clock,
	}, m.tenants, mem)
	if err != nil {
		return err
	}
	m.sched = sched
	return nil
}

// commitLocked 把当前调度器状态 + 推演进度原子写入检查点（持有 m.mu）。
func (m *Simulator) commitLocked() error {
	m.sched.mu.Lock()
	st := m.sched.exportStateLocked()
	m.sched.mu.Unlock()
	cp := &simCheckpoint{
		NextIdx: m.nextIdx,
		Time:    m.clock.Now(),
		Records: append([]StepRecord{}, m.records...),
		State:   st,
	}
	if err := m.store.Save(cp); err != nil {
		return err
	}
	m.state = st
	return nil
}

// Apply 按顺序应用一条事件并返回本步记录。
// 提交被拒、作业不存在等属于正常推演结论，会记录在 StepRecord 中并正常生效；
// 仅当检查点落盘失败时返回错误，此时本步对外等同于未发生（内存已回滚），
// 原样重试不会产生重复占位或状态错位。
func (m *Simulator) Apply(ev Event) (*StepRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrSimClosed
	}

	prevState := m.state
	prevTime := m.clock.Now()

	rec := StepRecord{Index: m.nextIdx, Event: ev}
	if err := m.applyEventLocked(ev, &rec); err != nil {
		// 事件本身非法（如时间倒退）：未触碰任何状态，直接返回。
		return nil, err
	}
	rec.Time = m.clock.Now()
	rec.Snapshot = m.sched.Snapshot()

	m.nextIdx++
	m.records = append(m.records, rec)
	if err := m.commitLocked(); err != nil {
		// 落盘失败：回滚到上一步已提交状态，本步对外等同于未发生。
		m.nextIdx--
		m.records = m.records[:len(m.records)-1]
		m.clock.t = prevTime
		m.state = prevState
		if rerr := m.rebuild(); rerr != nil {
			return nil, errors.Join(err, rerr)
		}
		return nil, err
	}
	return &rec, nil
}

// applyEventLocked 把事件应用到内部调度器（持有 m.mu）。
func (m *Simulator) applyEventLocked(ev Event, rec *StepRecord) error {
	switch ev.Type {
	case EventSubmit:
		res, err := m.sched.Submit(ev.JobID, ev.TenantID, ev.Priority, ev.Request)
		if err != nil {
			if re, ok := err.(*RejectError); ok {
				rec.RejectKind = RejectKindName(re.Kind)
			} else {
				rec.Err = err.Error()
			}
			return nil
		}
		rec.Decisions = res.Decisions
	case EventComplete:
		decisions, err := m.completeAll(ev.JobID)
		if err != nil {
			rec.Err = err.Error()
			return nil
		}
		rec.Decisions = decisions
	case EventCancel:
		decisions, err := m.cancelAll(ev.JobID)
		if err != nil {
			rec.Err = err.Error()
			return nil
		}
		rec.Decisions = decisions
	case EventAdvance:
		if ev.Time.Before(m.clock.Now()) {
			return ErrSimBackwardTime
		}
		m.clock.t = ev.Time
		decisions, err := m.sched.Tick(ev.Time)
		if err != nil {
			rec.Err = err.Error()
			return nil
		}
		rec.Decisions = decisions
	default:
		return ErrInvalidArgument
	}
	return nil
}

// completeAll 在内部调度器上执行 Complete 并返回全部裁决（含调度级联）。
func (m *Simulator) completeAll(jobID string) ([]Decision, error) {
	s := m.sched
	s.mu.Lock()
	defer s.mu.Unlock()
	decisions, err := s.completeLocked(jobID)
	if err != nil {
		return nil, err
	}
	if err := s.persistLocked(); err != nil {
		return nil, err
	}
	return decisions, nil
}

// cancelAll 在内部调度器上执行 Cancel 并返回全部裁决（含调度级联）。
func (m *Simulator) cancelAll(jobID string) ([]Decision, error) {
	s := m.sched
	s.mu.Lock()
	defer s.mu.Unlock()
	decisions, err := s.cancelLocked(jobID)
	if err != nil {
		return nil, err
	}
	if err := s.persistLocked(); err != nil {
		return nil, err
	}
	return decisions, nil
}

// Records 返回已生效的全部步骤记录（副本）。
func (m *Simulator) Records() []StepRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]StepRecord{}, m.records...)
}

// Snapshot 返回推演当前状态快照。
func (m *Simulator) Snapshot() *Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sched.Snapshot()
}

// Close 释放推演器资源（不影响任何已落盘的检查点）。
func (m *Simulator) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	_ = m.sched.Close()
	return m.store.Close()
}

// --- 推演检查点持久化 ---

// simCheckpoint 是推演的可恢复进度：已生效步骤 + 调度器状态 + 逻辑时间。
type simCheckpoint struct {
	NextIdx int
	Time    time.Time
	Records []StepRecord
	State   *persistedState
}

// simCheckpointStore 抽象推演检查点的持久化。
type simCheckpointStore interface {
	Save(cp *simCheckpoint) error
	Load() (*simCheckpoint, error)
	Close() error
}

// simCheckpointFile 是检查点文件名（与调度器状态文件互不干扰）。
const simCheckpointFile = "sim-checkpoint.json"

// simStoreFor 按配置创建检查点存储；WorkDir 为空时为纯内存（不持久化）。
func simStoreFor(cfg SimConfig) (simCheckpointStore, error) {
	if cfg.store != nil {
		return cfg.store, nil
	}
	if cfg.WorkDir == "" {
		return nopSimStore{}, nil
	}
	if err := os.MkdirAll(cfg.WorkDir, 0o755); err != nil {
		return nil, err
	}
	return &fileSimStore{path: filepath.Join(cfg.WorkDir, simCheckpointFile)}, nil
}

// fileSimStore 与调度器状态存储同款：JSON + 临时文件原子重命名，
// 保证崩溃后要么读到上一步、要么读到本步，不会读到半成品。
type fileSimStore struct{ path string }

func (s *fileSimStore) Save(cp *simCheckpoint) error {
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *fileSimStore) Load() (*simCheckpoint, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var cp simCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, err
	}
	return &cp, nil
}

func (s *fileSimStore) Close() error { return nil }

// nopSimStore 不持久化（纯内存推演）。
type nopSimStore struct{}

func (nopSimStore) Save(*simCheckpoint) error     { return nil }
func (nopSimStore) Load() (*simCheckpoint, error) { return nil, nil }
func (nopSimStore) Close() error                  { return nil }
