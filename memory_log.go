package go_raft

import (
	"errors"
	. "github.com/fuyao-w/common-util"
	"github.com/fuyao-w/deepcopy"
)

type memLog struct {
	firstIndex, lastIndex uint64
	log                   map[uint64]*LogEntry
}
type MemorySore struct {
	kv  *LockItem[map[string]interface{}]
	log *LockItem[memLog]
}

func newMemoryStore() MemorySore {
	return MemorySore{
		kv: NewLockItem(map[string]interface{}{}),
		log: NewLockItem(memLog{
			log: map[uint64]*LogEntry{},
		}),
	}
}

var errNotExist = errors.New("not exist")

func (m *MemorySore) Get(key string) (val string, err error) {
	kv := m.kv.Lock()
	defer m.kv.Unlock()

	v, ok := (*kv)[key]
	if ok {
		return v.(string), nil
	}
	return "", errNotExist
}

func (m *MemorySore) Set(key string, val string) error {
	m.kv.Action(func(t *map[string]interface{}) {
		(*t)[key] = val
	})
	return nil
}

func (m *MemorySore) SetUint64(key string, val uint64) error {
	m.kv.Action(func(t *map[string]interface{}) {
		(*t)[key] = val
	})
	return nil
}

func (m *MemorySore) GetUint64(key string) (uint64, error) {
	kv := m.kv.Lock()
	defer m.kv.Unlock()

	v, ok := (*kv)[key]
	if ok {
		return v.(uint64), nil
	}
	return 0, errNotExist
}
func (m *MemorySore) FirstIndex() (uint64, error) {
	var idx uint64
	m.log.Action(func(t *memLog) {
		idx = (*t).firstIndex
	})
	return idx, nil
}

func (m *MemorySore) LastIndex() (uint64, error) {
	var idx uint64
	m.log.Action(func(t *memLog) {
		idx = (*t).lastIndex
	})
	return idx, nil
}

func (m *MemorySore) GetLog(index uint64) (log *LogEntry, err error) {
	m.log.Action(func(t *memLog) {
		s := *t
		l, ok := s.log[index]
		if ok {
			log = deepcopy.Copy(l).(*LogEntry)
		} else {
			err = errNotExist
		}
	})
	return
}

func (m *MemorySore) SetLogs(logs []*LogEntry) error {
	m.log.Action(func(t *memLog) {
		s := *t
		for _, entry := range logs {
			s.log[entry.Index] = deepcopy.Copy(entry).(*LogEntry)
			if t.firstIndex == 0 {
				t.firstIndex = entry.Index
			}
			if entry.Index > t.lastIndex {
				t.lastIndex = entry.Index
			}
		}
	})
	return nil
}

func (m *MemorySore) DeleteRange(min, max uint64) error {
	m.log.Action(func(t *memLog) {
		s := *t
		for i := min; i <= max; i++ {
			delete(s.log, i)
		}
		if min <= s.firstIndex {
			s.firstIndex = max + 1
		}
		if max >= s.lastIndex {
			s.lastIndex = min - 1
		}
		if s.firstIndex > s.lastIndex {
			s.firstIndex = 0
			s.lastIndex = 0
		}
	})
	return nil
}
