package scheduler

import "fmt"

// RejectReason 提交被拒绝的可区分原因。
type RejectReason string

const (
	// ReasonInvalidRequest 请求非法：ID/租户名为空、优先级 < 0、
	// 资源需求为 0 或含负数、运行时长 <= 0。
	ReasonInvalidRequest RejectReason = "INVALID_REQUEST"
	// ReasonUnknownTenant 租户未在调度器注册。
	ReasonUnknownTenant RejectReason = "UNKNOWN_TENANT"
	// ReasonQuotaExceeded 资源需求超过该租户配额上限，即使全配额空闲也无法满足。
	ReasonQuotaExceeded RejectReason = "QUOTA_EXCEEDED"
	// ReasonCapacityFull 资源需求超过集群总容量，永远无法满足。
	ReasonCapacityFull RejectReason = "CAPACITY_FULL"
)

// SubmitError 提交拒绝错误。重复提交同一 ID 不属于错误（幂等返回已有作业）。
type SubmitError struct {
	Reason  RejectReason
	Message string
}

func (e *SubmitError) Error() string {
	return fmt.Sprintf("submit rejected [%s]: %s", e.Reason, e.Message)
}

// CancelReason 取消失败原因。
type CancelReason string

const (
	// CancelNotFound 作业不存在。
	CancelNotFound CancelReason = "NOT_FOUND"
	// CancelTerminal 作业已是终态（完成/取消），不可再取消。
	CancelTerminal CancelReason = "TERMINAL"
)

// CancelError 取消错误。
type CancelError struct {
	Reason  CancelReason
	Message string
}

func (e *CancelError) Error() string {
	return fmt.Sprintf("cancel failed [%s]: %s", e.Reason, e.Message)
}
