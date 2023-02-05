package go_raft

import (
	"golang.org/x/sync/errgroup"
	"sync/atomic"
)

type State uint64

const (
	Follower State = iota + 1
	Candidate
	Leader
	ShutDown
)

func newState() *State {
	state := new(State)
	state.setState(Follower)
	return state
}

func (s *State) setState(newState State) {
	atomic.StoreUint64((*uint64)(s), uint64(newState))
}

func (s *State) GetState() State {

	return State(atomic.LoadUint64((*uint64)(s)))
}
func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case ShutDown:
		return "ShutDown"
	default:
		return "Unknown"
	}
}

type raftContext struct {
	currentTerm uint64 // 任期
	// 临时记录
	commitIndex uint64
	lastApplied uint64

	state State // 当前状态
	//votedFor ServerID // 给谁投票了

	lastEntry                       *lockItem[lastEntry]
	candidateFromLeadershipTransfer bool
	// funcEg 跟踪与 Raft 相关的 goroutine
	funcEg *errgroup.Group
}

type lastEntry struct {
	lastSnapShotIndex, lastSnapShotTerm uint64
	lastLogIndex, lastLogTerm           uint64
}

func (r *raftContext) getLastLog() (term uint64, index uint64) {
	entry := r.lastEntry.Get()
	return entry.lastLogTerm, entry.lastLogIndex
}
func (r *raftContext) setLastLog(term uint64, index uint64) {
	r.lastEntry.Action(func(l *lastEntry) {
		l.lastLogIndex = index
		l.lastLogTerm = term
	})
}

func (r *raftContext) getLastSnapShot() (term uint64, index uint64) {
	entry := r.lastEntry.Get()
	return entry.lastSnapShotTerm, entry.lastSnapShotIndex
}
func (r *raftContext) setLastSnapShot(term uint64, index uint64) {
	r.lastEntry.Action(func(l *lastEntry) {
		l.lastSnapShotIndex = index
		l.lastSnapShotTerm = term
	})
}

func (r *raftContext) getLastLogEntry() (term uint64, index uint64) {
	entry := r.lastEntry.Get()
	if entry.lastLogIndex >= entry.lastSnapShotIndex {
		return entry.lastLogTerm, entry.lastLogIndex
	}
	return entry.lastSnapShotTerm, entry.lastSnapShotIndex
}

func (r *raftContext) setLastEntry(term uint64, index uint64) {
	r.lastEntry.Action(func(l *lastEntry) {
		l.lastLogTerm = term
		l.lastLogIndex = index
	})
}
func (r *raftContext) getLastIndex() (index uint64) {
	entry := r.lastEntry.Get()
	return max(entry.lastLogIndex, entry.lastSnapShotIndex)
}

func (r *raftContext) getCommitIndex() uint64 {
	return atomic.LoadUint64(&r.commitIndex)
}

func (r *raftContext) setCommitIndex(index uint64) {
	atomic.StoreUint64(&r.commitIndex, index)
}

func (r *Raft) setCurrentTerm(term uint64) {
	atomic.StoreUint64(&r.currentTerm, term)
}

func (r *Raft) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&r.currentTerm)
}
func (r *raftContext) getLastApplied() uint64 {
	return atomic.LoadUint64(&r.lastApplied)
}

func (r *raftContext) setLastApplied(index uint64) {
	atomic.StoreUint64(&r.lastApplied, index)
}

func (r *raftContext) goFunc(f func()) {
	r.funcEg.Go(func() error {
		f()
		return nil
	})
}

func (r *Raft) waitShutDown() {
	r.funcEg.Wait()
}

func (r *Raft) ShutDown() (resp defaultFuture) {
	r.shutDown.state.Action(func(t *bool) {
		if *t {
			resp = &shutDownFuture{}
		} else {
			close(r.shutDown.C)
			*t = true
			r.setShutDown()
			resp = &shutDownFuture{r}
		}
	})
	return resp
}
