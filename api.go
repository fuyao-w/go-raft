package go_raft

import (
	"errors"
	"fmt"
	. "github.com/fuyao-w/common-util"
	"time"
)

var (
	ErrPipelineReplicationNotSupported = errors.New("pipeline replication not supported")
	ErrNotFoundLog                     = errors.New("not found log")
	ErrNotLeader                       = errors.New("not leader")
	ErrCantBootstrap                   = errors.New("bootstrap only works on new clusters")
	ErrShutDown                        = errors.New("shut down")
	ErrLeadershipTransferInProgress    = errors.New("leader ship transfer in progress")
	// ErrAbortedByRestore is returned when a leader fails to commit a log
	// entry because it's been superseded by a user snapshot restore.
	ErrAbortedByRestore = errors.New("snapshot restored while committing log")
	ErrEnqueueTimeout   = errors.New("timed out enqueuing operation")
	ErrTimeout          = errors.New("time out")
	ErrPipelineShutdown = errors.New("append pipeline closed")
	ErrNotVoter         = errors.New("not voter")
)

// Raft 运行上下文
type Raft struct {
	latestConfiguration *AtomicVal[configuration]
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
	//commitment commitment
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
	// configurationsGetCh 用于从外部安全的获取配置信息
	configurationsGetCh chan *configurationsGetFuture
	// verifyCh 用于外部确定当前节点是否还是 leader
	verifyCh             chan *verifyFuture
	bootstrapCh          chan *bootstrapFuture
	leadershipTransferCh chan *leadershipTransferFuture
	userRestoreCh        chan *userRestoreFuture
	// leaderNotifyCh 当有 leader 相关的配置变更时，通过此通道进行通知
	leaderNotifyCh chan struct{}
	// conf  配置
	conf *AtomicVal[*Conf]
	// localAddr 当前节点身份和地址信息
	localAddr ServerInfo
	// clusterMember 集群的其他成员
	//clusterMember []*ServerInfo //
	// shutDown 停机组件
	shutDown shutDown
	// 上次与leader 联系的时间
	lastContact *LockItem[time.Time] // 上次与 leader 联系的时间
	// leaderState 状态上下文
	leaderState *LeaderState

	configurations configurations
	// leaderCh 当前节点成为 leader 的通知
	leaderCh chan bool

	// leader 信息
	leaderInfo *LockItem[ServerInfo]
}

func (r *Raft) restoreSnapShot() {
	list, err := r.snapShotStore.List()
	if err != nil {
		panic(err)
	}
	for _, meta := range list {
		if r.tryStoreSingleSnapShot(meta) != nil {
			continue
		}

		r.setLastApplied(meta.Index)
		r.setLastSnapShot(meta.Term, meta.Index)

		r.setCommittedConfiguration(meta.configuration, meta.Index)
		r.setLatestConfiguration(meta.configuration, meta.Index)
		return
	}
	// If we had snapshots and failed to load them, its an error
	if len(list) > 0 {
		panic(fmt.Errorf("failed to load any existing snapshots"))
	}
}

func (r *Raft) tryStoreSingleSnapShot(meta *SnapShotMeta) error {
	_, rc, err := r.snapShotStore.Open(meta.ID)
	if err != nil {
		return err
	}
	defer rc.Close()
	err = fsmRestoreAndMeasure(r.fsm, rc, meta.Size)
	if err != nil {
		return err
	}
	return nil
}

func (r *Raft) BootstrapCluster(configuration configuration) defaultFuture {
	future := &bootstrapFuture{
		configuration: configuration,
	}
	future.init()
	select {
	case <-r.shutDown.C:
		future.fail(ErrShutDown)
	case r.bootstrapCh <- future:
	}
	return future
}

func (r *Raft) Leader() ServerAddr {
	info := r.getLeaderInfo()
	return info.Addr
}
func (r *Raft) Apply(data []byte, timeout time.Duration) ApplyFuture {
	return r.applyLog(&LogEntry{Data: data}, timeout)
}
func (r *Raft) applyLog(entry *LogEntry, timeout time.Duration) ApplyFuture {
	var tm <-chan time.Time
	if timeout > 0 {
		tm = time.After(timeout)
	}
	var applyFuture = &LogFuture{
		log: entry,
	}
	applyFuture.init()
	select {
	case <-tm:
		return &errFuture[nilRespFuture]{errors.New("apply log time out")}
	case <-r.shutDown.C:
		return &errFuture[nilRespFuture]{ErrShutDown}
	case r.applyCh <- applyFuture:
		return applyFuture
	}
}

func (r *Raft) VerifyLeader() defaultFuture {
	vf := &verifyFuture{}
	vf.init()
	select {

	case <-r.shutDown.C:
		return &errFuture[nilRespFuture]{ErrShutDown}
	case r.verifyCh <- vf:
		return vf
	}
}

func (r *Raft) GetConfiguration() *configurationGetFuture {
	cf := &configurationGetFuture{}
	cf.init()
	cf.responded(r.latestConfiguration.Load(), nil)
	return cf
}

func (r *Raft) AddPeer(peer ServerAddr) defaultFuture {
	return r.requestConfigChange(configurationChangeRequest{
		command: AddVoter,
		peer: ServerInfo{
			ID:       ServerID(generateUUID()),
			Addr:     peer,
			Suffrage: Voter,
		},
		pervIndex: 0,
	}, 0)
}

func (r *Raft) requestConfigChange(req configurationChangeRequest, timeout time.Duration) IndexFuture {
	var tm <-chan time.Time
	if timeout > 0 {
		tm = time.After(timeout)
	}
	var ccf = &configurationChangeFuture{
		req: req,
	}
	ccf.init()
	select {
	case <-tm:
		return &errFuture[nilRespFuture]{err: errors.New("apply log time out")}
	case <-r.shutDown.C:
		return &errFuture[nilRespFuture]{err: ErrShutDown}
	case r.configurationChangeCh <- ccf:
		return ccf
	}
}

func (r *Raft) RemovePeer(peer ServerAddr) defaultFuture {
	return r.requestConfigChange(configurationChangeRequest{
		command: removeServer,
		peer: ServerInfo{
			Addr: peer,
		},
	}, 0)
}

func (r *Raft) AddVoter(id ServerID, address ServerAddr, prevIndex uint64, timeout time.Duration) IndexFuture {
	return r.requestConfigChange(configurationChangeRequest{
		command: AddVoter,
		peer: ServerInfo{
			Suffrage: 0,
			ID:       id,
			Addr:     address,
		},
		pervIndex: prevIndex,
	}, timeout)
}

func (r *Raft) AddNonVoter(id ServerID, address ServerAddr, prevIndex uint64, timeout time.Duration) IndexFuture {
	return r.requestConfigChange(configurationChangeRequest{
		command: AddNonVoter,
		peer: ServerInfo{
			Suffrage: 0,
			ID:       id,
			Addr:     address,
		},
		pervIndex: prevIndex,
	}, timeout)
}

func (r *Raft) RemoveVoter(id ServerID, addr ServerAddr, prevIndex uint64, timeout time.Duration) IndexFuture {
	return r.requestConfigChange(configurationChangeRequest{
		command: removeServer,
		peer: ServerInfo{
			Suffrage: 0,
			ID:       id,
			Addr:     addr,
		},
		pervIndex: prevIndex,
	}, timeout)
}

func (r *Raft) demoteVoter(id ServerID, address ServerAddr, prevIndex uint64, timeout time.Duration) IndexFuture {
	return r.requestConfigChange(configurationChangeRequest{
		command: DemoteVoter,
		peer: ServerInfo{
			Suffrage: 0,
			ID:       id,
			Addr:     address,
		},
		pervIndex: prevIndex,
	}, timeout)
}

func (r *Raft) SnapShot() Future[SnapShotFutureResp] {
	sf := &reqSnapShotFuture{}
	sf.init()
	select {

	case <-r.shutDown.C:
		return &errFuture[SnapShotFutureResp]{ErrShutDown}
	case r.fsmSnapshotCh <- sf:
		return sf
	}
}

func (r *Raft) LeaderCh() chan bool {
	return r.leaderCh
}
func (r *Raft) LastContact() time.Time {
	return r.lastContact.Get()
}

func (r *Raft) LastIndex() uint64 {
	return r.getLastIndex()
}

func (r *Raft) LastApplied() uint64 {
	return r.getLastApplied()
}

func (r *Raft) LeaderTransfer() defaultFuture {
	return r.initiateLeadershipTransfer(nil, nil)
}

func (r *Raft) initiateLeadershipTransfer(id *ServerID, address *ServerAddr) defaultFuture {
	future := &leadershipTransferFuture{
		ServerInfo: &ServerInfo{

			ID:   *id,
			Addr: *address,
		},
	}
	if *id == r.localAddr.ID {
		future.fail(errors.New("can't transfer to itself"))
		return future
	}
	future.init()
	select {
	case r.leadershipTransferCh <- future:
		return future
	case <-r.shutDown.C:
		return &errFuture[nilRespFuture]{ErrShutDown}
	default:
		return &errFuture[nilRespFuture]{ErrEnqueueTimeout}
	}
}

func (r *Raft) FastTimeOut() {
	r.setCandidate()
	r.candidateFromLeadershipTransfer = true
}
