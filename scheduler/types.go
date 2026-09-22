package scheduler

import "fmt"

// Resources 表示计算资源量（CPU 毫核 / 内存 MB）。
// 本系统以进程内资源池模拟集群计算资源，资源为标量两类：
// CPU 毫核（1000 = 1 vCPU）与内存 MB。
type Resources struct {
	CPUMillis int64
	MemoryMB  int64
}

// Add 返回 r+o。
func (r Resources) Add(o Resources) Resources {
	return Resources{CPUMillis: r.CPUMillis + o.CPUMillis, MemoryMB: r.MemoryMB + o.MemoryMB}
}

// Sub 返回 r-o（调用方需保证不产生负数，用于容量/配额判定时先 FitsIn）。
func (r Resources) Sub(o Resources) Resources {
	return Resources{CPUMillis: r.CPUMillis - o.CPUMillis, MemoryMB: r.MemoryMB - o.MemoryMB}
}

// FitsIn 判断 r 是否能放入 limit（任一维度超出即不可）。
func (r Resources) FitsIn(limit Resources) bool {
	return r.CPUMillis <= limit.CPUMillis && r.MemoryMB <= limit.MemoryMB
}

// Any 是否有任意一项为正。
func (r Resources) Any() bool {
	return r.CPUMillis > 0 || r.MemoryMB > 0
}

func (r Resources) String() string {
	return fmt.Sprintf("(cpu=%dm,mem=%dMB)", r.CPUMillis, r.MemoryMB)
}

// JobState 作业生命周期状态。
type JobState int

const (
	// StatePending 排队中（含被抢占后等待重新入队的作业）。
	StatePending JobState = iota
	// StateRunning 已分配资源、正在运行。
	StateRunning
	// StateCompleted 已正常完成（终态）。
	StateCompleted
	// StateCanceled 已取消（终态）。
	StateCanceled
)

func (s JobState) String() string {
	switch s {
	case StatePending:
		return "PENDING"
	case StateRunning:
		return "RUNNING"
	case StateCompleted:
		return "COMPLETED"
	case StateCanceled:
		return "CANCELED"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(s))
	}
}

// Terminal 是否为终态。终态作业不会被再次调度。
func (s JobState) Terminal() bool {
	return s == StateCompleted || s == StateCanceled
}

// Tenant 租户及其配额上限。
type Tenant struct {
	Name  string
	Quota Resources
}

// JobSpec 提交作业的请求参数。
type JobSpec struct {
	// ID 作业全局唯一标识，由提交方保证；重复提交同一 ID 不会重复占位。
	ID string
	// Tenant 所属租户名（必须已注册）。
	Tenant string
	// Priority 基础优先级，越大越高，必须 >= 0。
	Priority int
	// Request 作业资源需求（被分配期间全程独占）。
	Request Resources
	// DurationTicks 作业需要运行的 tick 数（被抢占后重新调度时继续累计剩余时长）。
	DurationTicks int64
}

// Job 作业运行时记录。
type Job struct {
	ID       string
	Tenant   string
	Priority int
	Request  Resources
	Duration int64

	State        JobState
	EnqueueTick  int64 // 最近一次进入排队的 tick（首次提交或被抢占）
	StartTick    int64 // 最近一次开始/恢复运行的 tick
	FinishTick   int64 // 最近一次调度时计算的预计完成 tick
	EndTick      int64 // 进入终态的 tick；0 表示尚未进入终态
	StartEffPrio int64 // 最近一次被调度时冻结的有效优先级（用于被抢占判定）

	Remaining int64 // 距离完成还需运行的 tick 数（每次运行/抢占时更新）

	Runs      int // 被调度执行的次数（首次 + 每次抢占恢复）
	Preempted int // 被抢占的次数
}

// RemainingTicks 返回距离完成还需运行的 tick 数。
func (j *Job) RemainingTicks() int64 {
	if j.Remaining < 0 {
		return 0
	}
	return j.Remaining
}

// Clone 返回作业的深拷贝快照。
func (j *Job) Clone() *Job {
	if j == nil {
		return nil
	}
	cp := *j
	return &cp
}

// 老化（aging）参数：排队等待时间每增加 AgingIntervalTicks 个 tick，
// 有效优先级提升 AgingBonusPerInterval。
// 有效优先级 = 基础优先级 + floor(等待tick数 / AgingIntervalTicks) * AgingBonusPerInterval
// 该机制保证任何排队作业的有效优先级随时间无界增长，最终超过所有运行中作业
// （运行时冻结在调度时刻的值），因此不会被无限期饿死，同时等待时长的
// 计算只依赖 tick 数，保证相同初始状态与相同提交序列的调度结果完全一致。
const (
	AgingIntervalTicks    int64 = 10
	AgingBonusPerInterval int64 = 1
)

// EffectivePriority 计算作业在 atTick 时刻的有效优先级。
// running 为 true 时表示作业正在运行，此时冻结为 StartEffPrio。
func (j *Job) EffectivePriority(atTick int64) int64 {
	if j.State == StateRunning {
		return j.StartEffPrio
	}
	waited := atTick - j.EnqueueTick
	if waited < 0 {
		waited = 0
	}
	return int64(j.Priority) + (waited/AgingIntervalTicks)*AgingBonusPerInterval
}
