package go_raft

import (
	"encoding/binary"
	"hash/adler32"
	"io"
)

type logHash struct {
	lastHash []byte
}

func (l *logHash) Add(p []byte) {
	hasher := adler32.New()
	hasher.Write(l.lastHash)
	hasher.Write(p)
	l.lastHash = hasher.Sum(nil)
}

type menFSM struct {
	logHash
	logs                []*applyItem
	lastIndex, lastTerm uint64
}

func errFn(errs []error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
func (m *menFSM) Persist(sink SnapShotSink) error {
	return errFn([]error{
		binary.Write(sink, binary.LittleEndian, m.lastIndex),
		binary.Write(sink, binary.LittleEndian, m.lastTerm),
		func() error {
			_, err := sink.Write(m.lastHash)
			return err
		}(),
	})
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
	m.Add(entry.Data)
	m.logs = append(m.logs, &applyItem{
		index: entry.Index,
		term:  entry.Term,
		data:  append([]byte(nil), entry.Data...),
	})
	return nil
}

func (m *menFSM) SnapShot() (FSMSnapShot, error) {
	return &*m, nil
}

func (m *menFSM) ReStore(rc io.ReadCloser) error {
	return errFn([]error{
		binary.Read(rc, binary.LittleEndian, m.lastIndex),
		binary.Read(rc, binary.LittleEndian, m.lastTerm),
		func() error {
			m.lastHash = make([]byte, adler32.Size)
			_, err := rc.Read(m.lastHash)
			return err
		}(),
	})
}
