package go_raft

//
type MemoryLog struct {
	logs []*LogEntry
}

func (m MemoryLog) FirstIndex() (uint64, error) {
	if len(m.logs) == 0 {
		return 0, nil
	}
	return m.logs[0].Index, nil
}

func (m MemoryLog) LastIndex() (uint64, error) {
	if len(m.logs) == 0 {
		return 0, nil
	}
	return m.logs[len(m.logs)-1].Index, nil
}

func (m MemoryLog) GetLog(index uint64) (log *LogEntry, err error) {
	for _, entry := range m.logs {
		if entry.Index == index {
			return entry, nil
		}
	}
	return nil, nil
}

func (m MemoryLog) SetLogs(log []*LogEntry) error {
	length := len(m.logs)
	for i, entry := range log {
		entry.Index = uint64(length + i)
	}
	m.logs = append(m.logs, log...)
	return nil
}

func (m MemoryLog) DeleteRange(min, max uint64) error {
	var newLogs []*LogEntry
	for _, log := range m.logs {
		if log.Index >= min && log.Index <= max {
			continue
		}
		newLogs = append(newLogs, log)
	}
	m.logs = newLogs
	return nil
}
