package go_raft

import (
	"fmt"
	"net"
	"time"
)

type Conf struct {
	LocalAddr         net.Addr
	LocalID           ServerID
	HeartBeatCycle    time.Duration
	MemberList        []Member
	HeartBeatTimeout  time.Duration
	SnapshotInterval  time.Duration
	ElectionTimeout   time.Duration
	CommitTimeout     time.Duration
	LeaderShipTimeout time.Duration // 担任领导角色后的超时时间，如果在此时间内没有达到法定人数的支持，则应该回退到  follower
	// MaxAppendEntries 单次提交支持的最长批量日志长度
	MaxAppendEntries  int
	SnapshotThreshold uint64
	// TrailingLogs controls how many logs we leave after a snapshot. This is used
	// so that we can quickly replay logs on a follower instead of being forced to
	// send an entire snapshot. The value passed here is the initial setting used.
	// This can be tuned during operation using ReloadConfig.
	TrailingLogs uint64
	// If we are a member of a cluster, and RemovePeer is invoked for the
	// local node, then we forget all peers and transition into the follower state.
	// If ShutdownOnRemove is set, we additional shutdown Raft. Otherwise,
	// we can become a leader of a cluster containing only this node.
	ShutdownOnRemove   bool
	MaxPool            int
	TransportTimeout   time.Duration
	NetLayer           NetLayer
	ServerAddrProvider ServerAddrProvider
	Logger             Logger
}

type Member struct {
	Addr net.Addr
}

var Config = &Conf{
	LocalAddr: func() net.Addr {
		addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8888")
		if err != nil {
			panic(fmt.Errorf("ResolveTCPAddr err :%s", err))
		}
		return addr
	}(),
	HeartBeatCycle: time.Second * 5,
}

func validateConf(config *Conf) error {
	ef := fmt.Errorf
	if len(config.LocalID) == 0 {
		return ef("local id is nil")
	}
	if config.HeartBeatTimeout < 5*time.Millisecond {
		return fmt.Errorf("HeartbeatTimeout is too low")
	}
	if config.ElectionTimeout < 5*time.Millisecond {
		return fmt.Errorf("ElectionTimeout is too low")
	}
	if config.CommitTimeout < time.Millisecond {
		return fmt.Errorf("CommitTimeout is too low")
	}
	if config.MaxAppendEntries <= 0 {
		return fmt.Errorf("MaxAppendEntries must be positive")
	}
	if config.MaxAppendEntries > 1024 {
		return fmt.Errorf("MaxAppendEntries is too large")
	}
	if config.SnapshotInterval < 5*time.Millisecond {
		return fmt.Errorf("SnapshotInterval is too low")
	}
	if config.LeaderShipTimeout < 5*time.Millisecond {
		return fmt.Errorf("LeaderLeaseTimeout is too low")
	}
	if config.LeaderShipTimeout > config.HeartBeatTimeout {
		return fmt.Errorf("LeaderLeaseTimeout (%s) cannot be larger than heartbeat timeout (%s)", config.LeaderShipTimeout, config.HeartBeatTimeout)
	}
	if config.ElectionTimeout < config.HeartBeatTimeout {
		return fmt.Errorf("ElectionTimeout (%s) must be equal or greater than Heartbeat Timeout (%s)", config.ElectionTimeout, config.HeartBeatTimeout)
	}
	return nil
}
