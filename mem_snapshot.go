package go_raft

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"time"
)

type memSnapshot struct {
	latest *memSnapshotSink
	has    bool
	sync.Mutex
}
type memSnapshotSink struct {
	meta *SnapShotMeta
	buf  *bytes.Buffer
}

func (m *memSnapshotSink) Write(p []byte) (n int, err error) {
	size, err := m.buf.Write(p)
	m.meta.Size += int64(size)
	return size, err
}

func (m *memSnapshotSink) Close() error {
	return nil
}

func (m *memSnapshotSink) ID() string {
	return m.meta.ID
}

func (m *memSnapshotSink) Cancel() error {
	return nil
}

func (m *memSnapshot) Open(id string) (*SnapShotMeta, io.ReadCloser, error) {
	m.Lock()
	defer m.Unlock()
	if !m.has {
		return nil, nil, errNotExist
	}
	if m.latest.meta.ID != id {
		return nil, nil, errNotExist
	}
	buffer := bytes.NewBuffer(m.latest.buf.Bytes())
	return m.latest.meta, ioutil.NopCloser(buffer), nil
}

func snapshotName(term, index uint64) string {
	now := time.Now()
	msec := now.UnixNano() / int64(time.Millisecond)
	return fmt.Sprintf("%d-%d-%d", term, index, msec)
}

func (m *memSnapshot) List() ([]*SnapShotMeta, error) {
	m.Lock()
	defer m.Unlock()
	if !m.has {
		return nil, nil
	}
	return []*SnapShotMeta{m.latest.meta}, nil
}

func (m *memSnapshot) Create(version SnapShotVersion, index, term uint64, configuration configuration, configurationIndex uint64, rpc RpcInterface) (SnapShotSink, error) {
	m.Lock()
	defer m.Unlock()
	sink := memSnapshotSink{
		meta: &SnapShotMeta{
			Version:            version,
			ID:                 snapshotName(term, index),
			Index:              index,
			Term:               term,
			configuration:      configuration,
			configurationIndex: configurationIndex,
			Size:               0,
		},
		buf: &bytes.Buffer{},
	}
	m.has = true
	m.latest = &sink
	return &sink, nil
}
