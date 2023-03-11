package go_raft

import (
	. "github.com/fuyao-w/common-util"

	"time"
)

type (
	leaderState struct {
		currentTerm uint64
		replState   map[string]*followerReplication
		stepDown    chan struct{}
	}
	followerReplication struct {
		failures           uint64
		closeHeartbeatCh   chan struct{}
		term               uint64
		nextIndex          uint64
		triggerCh          chan struct{}
		triggerDeferRespCh chan *defaultDeferResponse
		stepDownCh         chan struct{}
		stopCh             chan uint64
		server             *LockItem[ServerInfo]
		lastContact        *LockItem[time.Time]
		allowPipeline      bool
		notify             *LockItem[notifyMap]
		// notifyCh 立即发起心跳，用于验证当前节点是否还是领导人
		notifyCh chan struct{}
	}
	notifyMap map[*verifyFuture]struct{}
)

// notifyAll 通知所有的异步观察者结果
func (fr *followerReplication) notifyAll(succ bool) {
	fr.notify.Action(func(t *notifyMap) {
		for future, _ := range *t {
			future.vote(succ)
		}
		*t = make(notifyMap)
	})
}

func (fr *followerReplication) setLastContact() {
	fr.lastContact.Set(time.Now())
}

func (fr *followerReplication) close() {
	close(fr.stopCh)
	close(fr.closeHeartbeatCh)
}
