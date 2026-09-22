package scheduler

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// EventType WAL 事件类型。事件流是调度器状态的唯一真相来源：
// 重启时重放全部事件即可还原 tick、作业状态与资源占位。
type EventType string

const (
	EvTenantRegistered EventType = "TENANT_REGISTERED"
	EvSubmitted        EventType = "SUBMITTED"
	EvDispatched       EventType = "DISPATCHED"
	EvPreempted        EventType = "PREEMPTED"
	EvCompleted        EventType = "COMPLETED"
	EvCanceled         EventType = "CANCELED"
	EvTick             EventType = "TICK"
)

// Event 持久化事件（append-only WAL 的一条记录，JSONL 编码）。
type Event struct {
	Seq  int64     `json:"seq"`
	Tick int64     `json:"tick"`
	Type EventType `json:"type"`

	// 作业相关字段
	JobID       string    `json:"job_id,omitempty"`
	Tenant      string    `json:"tenant,omitempty"`
	Priority    int       `json:"priority,omitempty"`
	Request     Resources `json:"request,omitempty"`
	Duration    int64     `json:"duration,omitempty"`
	Remaining   int64     `json:"remaining,omitempty"`
	StartTick   int64     `json:"start_tick,omitempty"`
	FinishTick  int64     `json:"finish_tick,omitempty"`
	EffPriority int64     `json:"eff_priority,omitempty"`
	NextEnqueue int64     `json:"next_enqueue,omitempty"`
	Cause       string    `json:"cause,omitempty"`

	// 租户相关字段
	Quota Resources `json:"quota,omitempty"`
}

// Store 事件日志存储抽象。Append 必须保证落盘（fsync）后才返回，
// 这样已受理（返回成功）的提交在崩溃后不会丢失。
type Store interface {
	Append(ev *Event) error
	Read() ([]*Event, error)
	Close() error
}

// FileStore 基于本地 JSONL 文件 + fsync 的 WAL 存储。
// 以追加写方式工作，单文件顺序写；所有方法可被多 goroutine 并发调用。
type FileStore struct {
	mu   sync.Mutex
	path string
	f    *os.File
}

// NewFileStore 打开（不存在则创建）WAL 文件。
func NewFileStore(path string) (*FileStore, error) {
	if path == "" {
		return nil, errors.New("store path is empty")
	}
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create wal dir: %w", err)
		}
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal file: %w", err)
	}
	return &FileStore{path: path, f: f}, nil
}

func (s *FileStore) Append(ev *Event) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	data = append(data, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.f.Write(data); err != nil {
		return fmt.Errorf("write wal: %w", err)
	}
	// fsync：进程异常退出（甚至机器掉电）后已确认的事件不丢失。
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("sync wal: %w", err)
	}
	return nil
}

func (s *FileStore) Read() ([]*Event, error) {
	s.mu.Lock()
	path := s.path
	s.mu.Unlock()

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open wal for read: %w", err)
	}
	defer f.Close()

	var events []*Event
	scanner := bufio.NewScanner(f)
	// 单行为一个事件，预留足够 buffer。
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		raw := scanner.Bytes()
		if len(raw) == 0 {
			continue
		}
		ev := &Event{}
		if err := json.Unmarshal(raw, ev); err != nil {
			return nil, fmt.Errorf("corrupt wal at line %d: %w", line, err)
		}
		events = append(events, ev)
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("read wal: %w", err)
	}
	return events, nil
}

func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

// MemoryStore 进程内存储（测试/无持久化模式使用）。
type MemoryStore struct {
	mu     sync.Mutex
	events []*Event
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

func (s *MemoryStore) Append(ev *Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *ev
	s.events = append(s.events, &cp)
	return nil
}

func (s *MemoryStore) Read() ([]*Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Event, len(s.events))
	for i, ev := range s.events {
		cp := *ev
		out[i] = &cp
	}
	return out, nil
}

func (s *MemoryStore) Close() error { return nil }
