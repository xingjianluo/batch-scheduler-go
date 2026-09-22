package scheduler

import "fmt"

// RejectKindName 返回拒绝类别的可读名称。
func RejectKindName(k RejectKind) string {
	switch k {
	case RejectInvalidRequest:
		return "InvalidRequest"
	case RejectTenantNotFound:
		return "TenantNotFound"
	case RejectDuplicateJob:
		return "DuplicateJob"
	case RejectExceedsTenantQuota:
		return "ExceedsTenantQuota"
	case RejectExceedsClusterCapacity:
		return "ExceedsClusterCapacity"
	default:
		return "Unknown"
	}
}

// RejectError 表示作业提交被拒绝，Kind 用于程序化区分原因。
type RejectError struct {
	Kind   RejectKind
	Reason string
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("submit rejected [%s]: %s", RejectKindName(e.Kind), e.Reason)
}

// IsReject 判断 err 是否为指定 Kind 的拒绝错误。
func IsReject(err error, kind RejectKind) bool {
	r, ok := err.(*RejectError)
	return ok && r.Kind == kind
}

func newReject(kind RejectKind, format string, args ...any) *RejectError {
	return &RejectError{Kind: kind, Reason: fmt.Sprintf(format, args...)}
}
