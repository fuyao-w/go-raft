package go_raft

import (
	"container/list"
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// Raft 运行上下文
type Raft struct {
	latestConfiguration atomic.Value
	// logStore 提供日志操作的能力
	logStore LogStore
	// kvStorage 存储一些需要持久化的字段
	kvStorage KVStorage
	// snapShotStore 提供快照操作的能力
	snapShotStore SnapShotStore
	// snapShotStore 提供快照操作的能力
	userSnapShotFutureCh chan *userSnapshotFuture
	// fsmSnapshotCh is used to trigger a new snapshot being taken
	fsmSnapshotCh chan *reqSnapShotFuture
	// raftContext 状态上下文
	raftContext
	fsm LogFSM
	// fsmMutateCh is used to send state-changing updates to the FSM. This
	// receives pointers to commitTuple structures when applying logs or
	// pointers to restoreFuture structures when restoring a snapshot. We
	// need control over the order of these operations when doing user
	// restores so that we finish applying any old log applies before we
	// take a user snapshot on the leader, otherwise we might restore the
	// snapshot and apply old logs to it that were in the pipe.
	fsmMutateCh chan interface{}

	// commitment 帮助保存，计算已提交的索引
	commitment commitment
	// rpc 提供 RPC 调用能力
	rpc RpcInterface // RPC 接口，提供了选举，追加日志等功能
	// cmdChan RPC 命令消息
	cmdChan chan *CMD
	// applyCh 用于异步提交日志到主线程
	applyCh chan *LogFuture
	// followerNotifyCh 当有 follower 或者 candidate 相关的配置变更时，通过此通道进行通知
	followerNotifyCh chan struct{}
	// configurationChangeCh
	configurationChangeCh chan *configurationChangeFuture
	// configurationGetCh 用于从外部安全的获取配置信息
	configurationGetCh chan *configurationGetFuture
	// verifyCh 用于外部确定当前节点是否还是 leader
	verifyCh             chan *verifyFuture
	bootstrapCh          chan *bootstrapFuture
	leadershipTransferCh chan *leadershipTransferFuture
	userRestoreCh        chan *userRestoreFuture
	// leaderNotifyCh 当有 leader 相关的配置变更时，通过此通道进行通知
	leaderNotifyCh chan struct{}
	// conf  配置
	conf *atomic.Value
	// localAddr 当前节点身份和地址信息
	localAddr ServerInfo
	// clusterMember 集群的其他成员
	//clusterMember []*ServerInfo //
	// shutDown 停机组件
	shutDown shutDown
	// 上次与leader 联系的时间
	lastContact lockItem[time.Time] // 上次与 leader 联系的时间
	// leaderState 状态上下文
	leaderState LeaderState

	configurations configurations
	// leaderCh 当前节点成为 leader 的通知
	leaderCh chan bool

	// leader 信息
	leaderInfo lockItem[ServerInfo]
}

func (r *Raft) getLeaderInfo() ServerInfo {
	return r.leaderInfo.Get()
}

func (r *Raft) restoreUserSnapshot(meta *SnapShotMeta, reader io.Reader) error {
	commitIndex := r.configurations.commitIndex
	latestIndex := r.configurations.latestIndex
	if commitIndex != latestIndex {
		return fmt.Errorf("cannot restore snapshot now, wait until the configuration entry at %v "+
			"has been applied (have applied %v)", latestIndex, commitIndex)
	}
	inflight := r.leaderState.inflight
	for ele := inflight.Front(); ele != nil; {
		lf := ele.Value.(*LogFuture)
		lf.fail(ErrAbortedByRestore)
		inflight.Remove(ele)
	}

	term := r.getCurrentTerm()
	lastIndex := r.getLastIndex()
	if meta.Index > lastIndex {
		lastIndex = meta.Index
	}
	lastIndex++

	sink, err := r.snapShotStore.Create(SnapShotVersionMin, lastIndex, term, r.configurations.latest, r.configurations.latestIndex, r.rpc)
	if err != nil {
		return err
	}

	written, err := io.Copy(sink, reader)
	if err != nil {
		sink.Cancel()
		return err
	}
	if written != meta.Size {
		sink.Cancel()
		return fmt.Errorf("failed to write snapshot, size didn't match (%d != %d)", written, meta.Size)
	}
	if err = sink.Close(); err != nil {
		return err
	}

	fu := restoreFuture{
		ID: sink.ID(),
	}
	fu.init()
	fu.ShutdownCh = r.shutDown.ch

	select {
	case r.fsmMutateCh <- fu:
	case <-r.shutDown.ch:
		return ErrShutDown
	}

	if _, err := fu.Response(); err != nil {
		panic(fmt.Errorf("user restore err :%s", err))
	}

	r.setLastLog(term, lastIndex)
	r.setLastApplied(lastIndex)
	r.setLastSnapShot(term, lastIndex)
	return nil
}
func (r *Raft) leadershipTransferInProgress() bool {
	return atomic.LoadInt32(&r.leaderState.leadershipTransferInProgress) == 1
}
func (r *Raft) setLastContact() {
	r.lastContact.Set(time.Now())
}

type configuration struct {
	servers []ServerInfo
}
type configurations struct {
	commit      configuration
	latest      configuration
	commitIndex uint64
	latestIndex uint64
}

func (r *Raft) buildRPCHeader(err error) *RPCHeader {
	header := &RPCHeader{
		ID:   r.localAddr.ID,
		Addr: r.localAddr.Addr,
	}
	if err != nil {
		header.ErrMsg = err.Error()
	}
	return header
}
func (r *Raft) Config() *Conf {
	c, _ := r.conf.Load().(*Conf)
	return c
}
func (r *Raft) SetConfig(c *Conf) {
	r.conf.Store(c)
}
func (c *configuration) Clone() (copy configuration) {
	copy.servers = append(copy.servers, c.servers...)
	return
}
func (c *configurations) Clone() configurations {
	res := configurations{
		commit:      c.commit.Clone(),
		latest:      c.latest.Clone(),
		commitIndex: c.commitIndex,
		latestIndex: c.latestIndex,
	}
	return res
}

type LeaderState struct {
	inflight *list.List
	// stepDown 在和 follower 交互后，leader 可能会下台
	stepDown  chan struct{}
	replState map[ServerID]*followerReplication
	// commitCh 通知有日志被提交，
	commitCh                     chan struct{}
	commitment                   *commitment
	notify                       map[*verifyFuture]struct{}
	leadershipTransferInProgress int32 // indicates that a leadership transfer is in progress.
}

func (r *Raft) Start() {
	go r.shutDown.WhatForShutDown()
	//go r.rpc.Start()

}

func (r *Raft) NewRaft(config *Conf) *Raft {
	cmdChan := make(chan *CMD)
	conf := new(atomic.Value)
	conf.Store(config)
	raft := &Raft{
		rpc: NewNetTransport(config,
			new(DefaultPackageParser),
			new(JsonCmdHandler),
			&ServerProcessor{cmdChan: cmdChan},
		),
		cmdChan: cmdChan,
		conf:    conf,
		shutDown: shutDown{
			dataBus: DataBus{},
			ch:      make(chan struct{}),
		},
	}
	raft.shutDown.AddCallback(func(event int, param interface{}) {
		raft.shutDown.ch <- struct{}{}
	})

	raft.goFunc(r.tick)
	raft.goFunc(r.runSnapShot)
	raft.goFunc(r.runLogFSM)
	return raft
}

// commitTuple is used to send an index that was committed,
// with an optional associated future that should be invoked.
type commitTuple struct {
	log    *LogEntry
	future *LogFuture
}
