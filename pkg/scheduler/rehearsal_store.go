package scheduler

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// rehearsalFileKind 写入推演存档 Kind 信封字段的固定标识。
const rehearsalFileKind = "batch-scheduler-rehearsal"

// rehearsalCheckpoint 是一轮推演的可恢复存档：
// NextSeq/Tenants/Jobs/QueuedOrder 与 persistedState 同构（JSON 同名键），
// 因而与已有调度器状态文件互相兼容：
//   - 普通调度器加载本文件时会忽略新增的信封/进度字段，仍可作为普通状态读取；
//   - 推演器读取普通状态文件时走专门的“种子”入口（NewRehearsalFromStateFile）。
//
// Version/Kind 是推演存档的信封标识。
type rehearsalCheckpoint struct {
	Version int    `json:"rehearsalVersion"`
	Kind    string `json:"rehearsalKind"`

	NextSeq     int64             `json:"NextSeq"`
	Tenants     map[string]Tenant `json:"Tenants"`
	Jobs        map[string]*Job   `json:"Jobs"`
	QueuedOrder []string          `json:"QueuedOrder"`

	// 推演自身的进度与调度参数（续跑时必须用同一套容量/老化参数）。
	Capacity     Resource     `json:"Capacity"`
	AgeIncrement int          `json:"AgeIncrement"`
	AgingPeriod  int64        `json:"AgingPeriodNanos"`
	LogicalTime  time.Time    `json:"LogicalTime"`
	Steps        []stepRecord `json:"Steps"`
}

// stepRecord 是一步推演结果的可持久化表示。
// 业务错误（提交拒绝、作业不存在等）与本步生效后的完整状态快照都属于
// “事件演算结果”的一部分，随存档一起保存，使重启续跑后重建的报告
// （含每一步当时的状态、提交的受理或拒绝结果、失败原因）与不中断时逐项一致。
type stepRecord struct {
	Event RehearsalEvent `json:"Event"`
	Time  time.Time      `json:"Time"`

	Decisions []Decision `json:"Decisions"`

	// Submit 事件且被受理时的结果；未被受理（拒绝）时全部留空，
	// 重启后据此还原“受理/拒绝”的原始结论。
	SubmitJobID string `json:"SubmitJobID,omitempty"`
	Started     bool   `json:"Started,omitempty"`
	SubmitJob   *Job   `json:"SubmitJob,omitempty"`

	// 本步生效后的状态快照（当时的状态），重启后历史步骤依然可见。
	Snapshot *Snapshot `json:"Snapshot,omitempty"`

	// 错误序列化：拒绝类错误按 Kind 还原；哨兵错误按名称还原；其余保留文本。
	HasError    bool       `json:"HasError,omitempty"`
	ErrIsReject bool       `json:"ErrIsReject,omitempty"`
	ErrKind     RejectKind `json:"ErrKind,omitempty"`
	ErrSentinel string     `json:"ErrSentinel,omitempty"`
	ErrText     string     `json:"ErrText,omitempty"`
}

// saveRehearsalCheckpoint 原子覆盖写入推演存档：
// 写临时文件 -> fsync -> rename，保证崩溃后只可能读到上一步或本步，
// 绝不会读到写了一半的存档。
func saveRehearsalCheckpoint(path string, cp *rehearsalCheckpoint) error {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
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
	return os.Rename(tmp, path)
}

// loadRehearsalCheckpoint 读取推演存档。
// 返回值 exists 表示文件是否存在；文件存在但不是推演存档时返回错误。
func loadRehearsalCheckpoint(path string) (cp *rehearsalCheckpoint, exists bool, err error) {
	data, rerr := os.ReadFile(path)
	if rerr != nil {
		if errors.Is(rerr, fs.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, rerr
	}
	var c rehearsalCheckpoint
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, true, err
	}
	if c.Version == 0 || c.Kind != rehearsalFileKind {
		return nil, true, ErrRehearsalNotACheckpoint
	}
	if c.Version != rehearsalStateVersion {
		return nil, true, ErrRehearsalUnsupportedVersion
	}
	if c.Tenants == nil {
		c.Tenants = make(map[string]Tenant)
	}
	if c.Jobs == nil {
		c.Jobs = make(map[string]*Job)
	}
	return &c, true, nil
}
