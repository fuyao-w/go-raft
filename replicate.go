package go_raft

import (
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
		server             lockItem[ServerInfo]
		lastContact        lockItem[time.Time]
		allowPipeline      bool
		notify             lockItem[notifyMap]
		// notifyCh is notified to send out a heartbeat, which is used to check that
		// this server is still leader.
		notifyCh chan struct{}
	}
)
type notifyMap map[*verifyFuture]struct{}
