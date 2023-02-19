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
		Term               uint64
		failures           uint64
		nextIndex          uint64
		triggerCh          chan struct{}
		triggerDeferRespCh chan *defaultDeferResponse
		stepDownCh         chan struct{}
		stopCh             chan uint64
		server             *LockItem[ServerInfo]
		lastContact        *LockItem[time.Time]
		allowPipeline      bool
		notify             *LockItem[notifyMap]
		// notifyCh is notified to send out a heartbeat, which is used to check that
		// this server is still leader.
		notifyCh chan struct{}
	}
)
type notifyMap map[*verifyFuture]struct{}
