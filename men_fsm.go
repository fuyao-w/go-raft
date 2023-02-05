package go_raft

import (
	"encoding/binary"
	"io"
)

type menFSM struct {
	logs                []*applyItem
	lastIndex, lastTerm uint64
}

func (m *menFSM) Persist(sink SnapShotSink) error {
	binary.Write(sink, binary.LittleEndian, m.lastIndex)
	binary.Write(sink, binary.LittleEndian, m.lastTerm)
	return nil
}

func (m *menFSM) Release() {
}

type applyItem struct {
	index uint64
	term  uint64
	data  []byte
}

func (m *menFSM) Apply(entry *LogEntry) interface{} {
	if entry.Index < m.lastIndex {
		panic("index error")
	}
	if entry.Term < m.lastTerm {
		panic("term error")
	}
	m.lastTerm = entry.Term
	m.lastIndex = entry.Index
	m.logs = append(m.logs, &applyItem{
		index: entry.Index,
		term:  entry.Term,
		data:  entry.Data,
	})
	return nil
}

func (m *menFSM) SnapShot() (FSMSnapShot, error) {
	return &*m, nil
}

func (m *menFSM) ReStore(closer io.ReadCloser) error {
	binary.Read(closer, binary.LittleEndian, m.lastIndex)
	binary.Read(closer, binary.LittleEndian, m.lastTerm)
	
	return nil
}
