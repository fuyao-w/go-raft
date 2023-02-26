package go_raft

type memStore struct {
	kv map[string]any
}

func (m *memStore) FirstIndex() (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (m *memStore) LastIndex() (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (m *memStore) GetLog(index uint64) (log *LogEntry, err error) {
	//TODO implement me
	panic("implement me")
}

func (m *memStore) GetLogRange(from, to uint64) (log []*LogEntry, err error) {
	//TODO implement me
	panic("implement me")
}

func (m *memStore) SetLogs(logs []*LogEntry) error {
	//TODO implement me
	panic("implement me")
}

func (m *memStore) DeleteRange(from, to uint64) error {
	//TODO implement me
	panic("implement me")
}
