package scheduler

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// Store 抽象调度状态的持久化。
type Store interface {
	// Save 原子地覆盖持久化全部状态。
	Save(state *persistedState) error
	// Load 读取最近一次持久化的状态；从未持久化时返回 (nil, nil)。
	Load() (*persistedState, error)
	// Close 释放底层资源。
	Close() error
}

// persistedState 是持久化的完整调度状态。
type persistedState struct {
	NextSeq     int64
	Tenants     map[string]Tenant
	Jobs        map[string]*Job
	QueuedOrder []string // 队列中的作业 ID，按 SubmitSeq 升序（被抢占作业保留原位）
}

// fileStore 基于 JSON + 临时文件原子重命名的文件存储。
// 单次 Save 即一次 fsync + rename，保证崩溃后要么读到旧状态、要么读到新状态。
type fileStore struct {
	path string
}

func newFileStore(path string) (Store, error) {
	if path == "" {
		return nopStore{}, nil
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	return &fileStore{path: path}, nil
}

func (s *fileStore) Save(state *persistedState) error {
	data, err := json.MarshalIndent(state, "", "  ")
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

func (s *fileStore) Load() (*persistedState, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var st persistedState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, err
	}
	if st.Tenants == nil {
		st.Tenants = make(map[string]Tenant)
	}
	if st.Jobs == nil {
		st.Jobs = make(map[string]*Job)
	}
	return &st, nil
}

func (s *fileStore) Close() error { return nil }

// nopStore 不做任何持久化。
type nopStore struct{}

func (nopStore) Save(*persistedState) error     { return nil }
func (nopStore) Load() (*persistedState, error) { return nil, nil }
func (nopStore) Close() error                   { return nil }
