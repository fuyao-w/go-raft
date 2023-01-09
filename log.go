package go_raft

import "time"

const (
	LogCommand LogType = iota
	LogBarrier
	// LogNoop 只用于确认 leader
	LogNoop
	LogConfiguration
)

type (
	LogType uint8
	// LogEntry 日志条目
	LogEntry struct {
		// Term 当前日志写入时的任期
		Term uint64 `json:"term"`
		// Data 当前日志写入的内容
		Data []byte `json:"data"`
		// Index 当前日志写入的索引
		Index uint64 `json:"index"`
		// Type 当前日志的类型
		Type LogType `json:"type"`
		// CreatedAt 当前日志的创建爱时间
		CreatedAt time.Time `json:"created_at"`
	}
)

// LogStore 提供日志操作的抽象
type LogStore interface {
	// FirstIndex 返回第一个写入的索引，-1 代表没有
	FirstIndex() (uint64, error)
	// LastIndex 返回最后一个写入的索引，-1 代表没有
	LastIndex() (uint64, error)
	// GetLog 返回指定位置的索引
	GetLog(index uint64) (log *LogEntry, err error)
	// GetLogRange 按指定范围遍历索引，闭区间
	GetLogRange(from, to uint64) (log []*LogEntry, err error)
	// SetLogs 追加日志
	SetLogs(logs []*LogEntry) error
	// DeleteRange 批量删除指定范围的索引内容，用于快照生成
	DeleteRange(from, to uint64) error
}
