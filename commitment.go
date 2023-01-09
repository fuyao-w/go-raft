package go_raft

import (
	"sync"
)

type commitment struct {
	lock        sync.Mutex
	commitCh    chan struct{}
	matchIndex  map[ServerID]uint64
	commitIndex uint64
	startIndex  uint64
}

func newCommitment(commitCh chan struct{}, configuration configuration, startIndex uint64) *commitment {
	return &commitment{
		commitCh:    commitCh,
		matchIndex:  map[ServerID]uint64{},
		commitIndex: 0,
		startIndex:  startIndex,
	}
}

func (c *commitment) setConfiguration(config configuration) {
	oldMatchIndex := c.matchIndex
	c.matchIndex = map[ServerID]uint64{}
	for _, server := range config.servers {
		if server.Suffrage == Voter {
			c.matchIndex[server.ID] = oldMatchIndex[server.ID]
		}
	}
	c.reCalculate()
}
func (c *commitment) GetCommitIndex() uint64 {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.commitIndex
}

func (c *commitment) match(id ServerID, matchIndex uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if prev, ok := c.matchIndex[id]; ok && matchIndex > prev {
		c.matchIndex[id] = matchIndex
		c.reCalculate()
	}
}
func (c *commitment) reCalculate() {
	if len(c.matchIndex) == 0 {
		return
	}
	matched := make([]uint64, 0, len(c.matchIndex))
	for _, idx := range c.matchIndex {
		matched = append(matched, idx)
	}
	sortSlice(matched)

	quorumMatchIndex := matched[len(matched)-1/2]
	if quorumMatchIndex > c.commitIndex && quorumMatchIndex >= c.startIndex {
		c.commitIndex = quorumMatchIndex
		asyncNotify(c.commitCh)
	}
}
