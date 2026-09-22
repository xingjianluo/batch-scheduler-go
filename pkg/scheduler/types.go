// Package scheduler 实现本地批处理作业调度系统：
// 通过进程内资源池模拟集群计算资源，通过本地持久化模拟集群状态存储，
// 支持多租户配额、优先级抢占、并发安全提交与崩溃恢复。
package scheduler

import "time"

// RejectKind 标识作业提交被拒绝的原因类别，便于调用方区分失败原因。
type RejectKind int

const (
	// RejectInvalidRequest 请求非法（字段缺失/为负等）。
	RejectInvalidRequest RejectKind = iota
	// RejectTenantNotFound 租户不存在。
	RejectTenantNotFound
	// RejectDuplicateJob 同一作业 ID 重复提交。
	RejectDuplicateJob
	// RejectExceedsTenantQuota 资源需求超出租户配额上限。
	RejectExceedsTenantQuota
	// RejectExceedsClusterCapacity 资源需求超过集群物理总容量。
	RejectExceedsClusterCapacity
)

// Resource 表示可调度资源量（CPU 核数、内存 MB、GPU 卡数）。
type Resource struct {
	CPU    int
	Memory int
	GPU    int
}

// JobState 作业生命周期状态。
type JobState int

const (
	// StateQueued 排队中（含被抢占后重新入队的作业）。
	StateQueued JobState = iota
	// StateRunning 运行中，持有资源占位。
	StateRunning
	// StateCompleted 已完成，终态。
	StateCompleted
	// StateCanceled 已取消，终态。
	StateCanceled
)

// String 返回状态的可读名称。
func (s JobState) String() string {
	switch s {
	case StateQueued:
		return "Queued"
	case StateRunning:
		return "Running"
	case StateCompleted:
		return "Completed"
	case StateCanceled:
		return "Canceled"
	default:
		return "Unknown"
	}
}

// Job 描述一个批处理作业及其调度状态。
type Job struct {
	ID          string
	TenantID    string
	Priority    int      // 基础优先级，数值越大优先级越高
	Request     Resource // 资源需求
	SubmitSeq   int64    // 全局单调提交序号，同优先级下 FIFO
	SubmittedAt time.Time

	State          JobState
	QueuedAt       time.Time // 最近一次进入队列的时间（用于老化与排序）
	PreemptedTimes int       // 被抢占次数（用于抢占裁决的平局打破）
	StartedAt      time.Time // 运行开始时间
	FinishedAt     time.Time // 进入终态时间
}

// Tenant 描述租户及其配额上限（瞬时持有资源上限）。
type Tenant struct {
	ID            string
	Quota         Resource
	MaxConcurrent int // 0 表示不限制并发作业数
}

// Config 是调度器配置。
type Config struct {
	Capacity     Resource
	AgeIncrement int           // 每个 AgingPeriod 等待获得的等效优先级
	AgingPeriod  time.Duration // 老化提升间隔
	StorePath    string        // 持久化文件路径；为空表示不持久化
	Clock        Clock         // 时钟，便于确定性测试；nil 表示使用系统时钟
}

// Clock 抽象时间来源。
type Clock interface {
	Now() time.Time
}

// Decision 描述一次调度裁决（用于日志与可解释性）。
type Decision struct {
	Time      time.Time
	JobID     string
	Action    string // "admit-queued" | "start" | "preempt" | "complete" | "cancel" | "reject"
	Reason    string
	JobState  JobState
	Used      Resource
	Capacity  Resource
	VictimIDs []string // 发生抢占时的牺牲品作业 ID
}

// Snapshot 是某一时刻调度器的只读状态快照（用于测试与日志）。
type Snapshot struct {
	Time       time.Time
	Capacity   Resource
	Used       Resource
	Jobs       []Job
	QueuedIDs  []string
	RunningIDs []string
	Tenants    []Tenant
	TenantUsed map[string]Resource
}
