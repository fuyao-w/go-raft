package go_raft

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	. "github.com/fuyao-w/common-util"
	"golang.org/x/sync/errgroup"
)

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
	for ele := inflight.Front(); ele != nil; ele.Next() {
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
	fu.ShutdownCh = r.shutDownCh()

	select {
	case r.fsmMutateCh <- fu:
	case <-r.shutDownCh():
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
	if r.leaderState.leadershipTransferInProgress == nil {
		return false
	}
	pointer := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&r.leaderState.leadershipTransferInProgress)))
	return *(*bool)(pointer)
}
func (r *Raft) setLeadershipTransferInProgress(inProgress bool) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&r.leaderState.leadershipTransferInProgress)),
		unsafe.Pointer(&inProgress))
}
func (r *Raft) setLastContact() {
	r.lastContact.Set(time.Now())
}

func (r *Raft) buildRPCHeader(err error) *RPCHeader {
	header := &RPCHeader{
		ID:   r.localInfo.ID,
		Addr: r.localInfo.Addr,
	}
	if err != nil {
		header.ErrMsg = err.Error()
	}
	return header
}

func (r *Raft) Config() *Conf {
	return r.conf.Load()
}

func (r *Raft) SetConfig(c *Conf) {
	r.conf.Store(c)
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
	leadershipTransferInProgress *bool // indicates that a leadership transfer is in progress.
}

func (l *LeaderState) close() {
	for _, replication := range l.replState {
		replication.close()
	}
	for e := l.inflight.Front(); e != nil; e.Next() {
		e.Value.(*LogFuture).fail(ErrLeadershipLost)
	}
	for future, _ := range l.notify {
		future.fail(ErrLeadershipLost)
	}
	*l = LeaderState{
		leadershipTransferInProgress: l.leadershipTransferInProgress,
	}
}
func (r *Raft) Start() {
	go r.shutDown.WaitForShutDown()
}

func NewRaft(config *Conf, logStore LogStore, fsm LogFSM, kvStorage KVStorage, snapShotStore SnapShotStore) *Raft {
	if err := validateConf(config); err != nil {
		panic(err)
	}
	currentTerm, err := kvStorage.GetUint64(keyCurrentTerm)
	if err != nil {
		panic(err)
	}

	lastIndex, err := logStore.LastIndex()
	if err != nil {
		panic(err)
	}
	var lastLog *LogEntry
	if lastIndex > 0 {
		lastLog, err = logStore.GetLog(lastIndex)
		if err != nil {
			panic(err)
		}
	}

	cmdChan := make(chan *CMD)

	raft := &Raft{
		latestConfiguration:  NewAtomicVal[configuration](),
		logStore:             logStore,
		kvStorage:            kvStorage,
		snapShotStore:        snapShotStore,
		userSnapShotFutureCh: make(chan *userSnapshotFuture),
		fsmSnapshotCh:        make(chan *reqSnapShotFuture),
		raftContext: raftContext{
			currentTerm:                     currentTerm,
			commitIndex:                     0,
			lastApplied:                     0,
			state:                           0,
			lastEntry:                       NewLockItem[lastEntry](),
			candidateFromLeadershipTransfer: false,
			funcEg:                          new(errgroup.Group),
		},
		fsm:                   fsm,
		fsmMutateCh:           make(chan interface{}),
		rpc:                   NewNetTransport(config),
		cmdChan:               cmdChan,
		applyCh:               make(chan *LogFuture),
		followerNotifyCh:      make(chan struct{}),
		configurationChangeCh: make(chan *configurationChangeFuture),
		configurationsGetCh:   make(chan *configurationsGetFuture),
		verifyCh:              make(chan *verifyFuture),
		bootstrapCh:           make(chan *bootstrapFuture),
		leadershipTransferCh:  make(chan *leadershipTransferFuture),
		userRestoreCh:         make(chan *userRestoreFuture),
		leaderNotifyCh:        make(chan struct{}),
		conf:                  NewAtomicVal[*Conf](),
		localInfo:             ServerInfo{},
		shutDown: shutDown{
			dataBus: DataBus{},
			C:       make(chan struct{}),
		},
		lastContact:    NewLockItem[time.Time](),
		leaderState:    new(LeaderState),
		configurations: configurations{},
		leaderCh:       make(chan bool),
		leaderInfo:     NewLockItem[ServerInfo](),
	}
	raft.SetConfig(config)
	raft.shutDown.AddCallback(func(event int, param interface{}) {
		raft.shutDown.C <- struct{}{}
	})

	raft.setCurrentTerm(currentTerm)
	raft.setLastLog(lastLog.Term, lastLog.Index)

	raft.restoreSnapShot()
	_, lastSnapShotIndex := raft.getLastSnapShot()
	for i := lastSnapShotIndex + 1; i < lastLog.Index; i++ {
		tLog, err := raft.logStore.GetLog(i)
		if err != nil {
			panic(err)
		}
		raft.processConfigurationLogEntry(tLog)
	}
	raft.rpc.SetHeartbeatFastPath(raft.processHeartBeat)
	raft.setFollower()
	raft.goFunc(
		raft.run,
		raft.runSnapShot,
		raft.runLogFSM,
	)
	return raft
}

// commitTuple is used to send an index that was committed,
// with an optional associated future that should be invoked.
type commitTuple struct {
	log    *LogEntry
	future *LogFuture
}

func (r *Raft) run() {
	for {
		select {
		case <-r.shutDownCh():
			return
		default:
			r.tick()
		}
	}
}

func (r *Raft) getState() State {
	return State(atomic.LoadUint64((*uint64)(&r.state)))
}
func (r *Raft) setState(newState State) {
	r.getLastLogEntry()
	atomic.StoreUint64((*uint64)(&r.state), (uint64)(newState))
}

func (r *Raft) setCommittedConfiguration(c configuration, index uint64) {
	r.configurations.commit = c
	r.configurations.commitIndex = index
}
func (r *Raft) setLatestConfiguration(c configuration, index uint64) {
	r.configurations.latest = c
	r.configurations.latestIndex = index
	r.latestConfiguration.Store(c.Clone())
}
func (r *Raft) clearLeaderInfo() {
	r.updateLeaderInfo(func(s *ServerInfo) {
		*s = ServerInfo{}
	})
}

func (r *Raft) updateLeaderInfo(act func(s *ServerInfo)) {
	r.leaderInfo.Action(act)
}
func DecodeConfiguration(data []byte) (c configuration) {
	json.Unmarshal(data, &c)
	return
}
func EncodeConfiguration(c configuration) (data []byte) {
	data, _ = json.Marshal(c)
	return
}
func (r *Raft) processConfigurationLogEntry(entry *LogEntry) error {
	r.setCommittedConfiguration(r.configurations.latest, r.configurations.latestIndex)
	r.setLatestConfiguration(DecodeConfiguration(entry.Data), entry.Index)
	return nil
}

// processAppendEntries 处理心跳
func (r *Raft) processHeartBeat(cmd *CMD) bool {
	switch req := cmd.Request.(type) {
	case *AppendEntryRequest:
		r.processAppendEntries(req, cmd)
		return true
	}
	return false
}

// processAppendEntries 处理日志提交
func (r *Raft) processAppendEntries(req *AppendEntryRequest, cmd *CMD) {
	var (
		lastTerm, lastIndex = r.getLastLogEntry()
		resp                = &AppendEntryResponse{
			Term:         r.getCurrentTerm(),
			LastLogIndex: lastIndex,
		}
		err error
	)
	getPrevLogTerm := func() (uint64, error) {
		if req.PrevLogIndex == lastIndex {
			return lastTerm, nil
		}
		logEntry, err := r.logStore.GetLog(req.PrevLogIndex)
		if err != nil {
			return 0, err
		}
		if logEntry == nil {
			return 0, ErrNotFoundLog
		}
		return logEntry.Term, nil
	}
	checkLogLegitimate := func() error {
		if req.PrevLogIndex <= 0 {
			return nil
		}
		// 校验日志
		prevLogTerm, err := getPrevLogTerm()
		if err != nil {
			return err
		}
		if req.PrevLogTerm != prevLogTerm {
			return ErrNotFoundLog
		}
		return nil
	}
	processLogConflict := func() (err error) {
		if len(req.Entries) == 0 {
			return
		}
		firstEntry := req.Entries[0]
		if firstEntry.Index == lastIndex {
			return nil
		}
		firstEntry, err = r.logStore.GetLog(firstEntry.Index)
		if err != nil {
			return ErrNotFoundLog
		}
		if firstEntry.Term != req.Term {
			if err = r.logStore.DeleteRange(firstEntry.Index, lastIndex); err != nil {
				return err
			}
			if firstEntry.Index <= r.configurations.latestIndex {
				r.setLatestConfiguration(r.configurations.latest, r.configurations.commitIndex)
			}
		}
		return nil
	}
	saveLogEntries := func() error {
		if n := len(req.Entries); n > 0 {
			if err := r.logStore.SetLogs(req.Entries); err != nil {
				return err
			}
			for _, entry := range req.Entries {
				if err := r.processConfigurationLogEntry(entry); err != nil {
					return err
				}
			}

			last := req.Entries[n-1]
			r.setLastEntry(last.Index, last.Term)
		}
		return nil
	}

	updateConfiguration := func() error {
		_, lastIndex = r.getLastLogEntry()
		if req.LeaderCommit > 0 && req.LeaderCommit > r.commitIndex {
			idx := Min(req.LeaderCommit, lastIndex)
			r.commitIndex = idx
			if r.configurations.latestIndex <= idx {
				r.setCommittedConfiguration(r.configurations.latest, idx)
			}
			// TODO process log
		}
		return nil
	}
	checkParam := func() error {
		if req.Term < r.getCurrentTerm() {
			return errors.New("term is too low")
		}
		if req.Term > r.getCurrentTerm() || ((r.state != Follower) && r.candidateFromLeadershipTransfer) {
			r.setCurrentTerm(req.Term)
			r.setFollower()
			resp.Term = req.Term
		}
		return nil
	}
	updateLeader := func() error {
		r.updateLeaderInfo(func(s *ServerInfo) {
			*s = ServerInfo{
				ID:   req.ID,
				Addr: req.Addr,
			}
		})
		return nil
	}
	funList := []func() error{
		checkParam,
		updateLeader,
		checkLogLegitimate,
		processLogConflict,
		saveLogEntries,
		updateConfiguration,
	}
	defer func() {
		resp.RPCHeader = r.buildRPCHeader(err)
		cmd.Response <- resp
	}()

	for _, f := range funList {
		if f() != nil {
			return
		}
	}

	resp.Success = true
	r.setLastContact()
}

// appendEntry 处理投票信息
func (r *Raft) appendEntry(req *AppendEntryRequest, cmd *CMD) {
	appendEntry := func() (succ bool) {
		if req.Term < r.currentTerm {
			return
		}
		log, err := r.logStore.GetLog(req.PrevLogIndex)
		if err != nil {
			return
		}
		if log == nil {
			return
		}
		if log.Term != req.PrevLogTerm {
			return
		}
		if len(r.getLeaderInfo().ID) == 0 {
			r.updateLeaderInfo(func(s *ServerInfo) {
				s.ID = req.LeaderID
			})
		}
		if err = r.logStore.SetLogs(req.Entries); err != nil {
			return
		}
		return true
	}

	cmd.Response <- &AppendEntryResponse{
		Term:    r.currentTerm,
		Success: appendEntry(),
	}
}

// fastTimeout leader 心跳快速超时,转换为 follower
func (r *Raft) fastTimeout(_ *FastTimeOutRequest, cmd *CMD) {
	r.clearLeaderInfo()
	r.setCandidate()
	r.candidateFromLeadershipTransfer = true
	cmd.Response <- &FastTimeOutResponse{}
}

// installSnapshot 安装快照
func (r *Raft) installSnapshot(req *InstallSnapshotRequest, cmd *CMD) {
	var (
		resp = new(InstallSnapshotResponse)
		err  error
	)
	defer func() {
		resp.RPCHeader = r.buildRPCHeader(err)
		io.Copy(ioutil.Discard, cmd.Reader)
	}()

	// 直接忽略即可
	if req.Term < r.getCurrentTerm() {
		r.logger.Infof("ingore install snapshot request with older term , request term :%d , current term :%d", req.Term, r.getCurrentTerm())
		return
	}
	// 更新自己的任期到最新
	if req.Term > r.getCurrentTerm() {
		r.setFollower()
		r.setCurrentTerm(req.Term)
		resp.Term = req.Term
	}
	// 更新 leader 信息
	r.updateLeaderInfo(func(s *ServerInfo) {
		*s = ServerInfo{
			ID: req.ID,
			Addr: func() ServerAddr {
				if len(req.ID) > 0 { // 说明是其他节点过来的，而不是人通过 api 引导的
					return r.rpc.DecodeAddr([]byte(req.RPCHeader.Addr))
				}
				// 人为引导的，使用请求里传过来的 leader 即可
				return r.rpc.DecodeAddr(req.Leader)
			}(),
		}
	})

	reqConfiguration := DecodeConfiguration(req.Configuration)

	sink, err := r.snapShotStore.Create(req.SnapShotVersion, req.LastLogIndex, req.LastLogTerm, reqConfiguration, req.ConfigurationIndex, r.rpc)
	if err != nil {
		r.logger.Errorf("failed to create snapshot sink ,err :%s", err)
		return
	}

	// 监控统计用
	counterReader := newCounterReader(cmd.Reader)
	written, err := io.Copy(sink, counterReader)
	if err != nil {
		sink.Cancel()
		return
	}

	if written != req.Size {
		sink.Cancel()
		err = errors.New("read not enough")
		return
	}
	if err = sink.Close(); err != nil {
		r.logger.Errorf("failed to close snapshot ,err :%s", err)
		return
	}
	r.logger.Infof("copied to local snapshot,bytes :%d", written)

	fu := &restoreFuture{
		ID: sink.ID(),
	}
	fu.init()
	fu.ShutdownCh = r.shutDownCh()
	select {
	case r.fsmMutateCh <- fu: // 交给状态机同步到最新的状态
	case <-r.shutDownCh():
		fu.responded(nil, ErrShutDown)
		return
	}
	// 等待状态机同步结果
	if _, err = fu.Response(); err != nil {
		r.logger.Errorf("failed to restore snapshot ,err :%s", err)
		return
	}

	r.setLastApplied(req.LastLogIndex)
	r.setLastSnapShot(req.LastLogIndex, req.LastLogTerm)
	r.setLatestConfiguration(reqConfiguration, req.ConfigurationIndex)
	r.setCommittedConfiguration(reqConfiguration, req.ConfigurationIndex)

	if _err := r.compactLogEntries(req.LastLogIndex); _err != nil {
		r.logger.Errorf("failed to compact logs ,err :%s", _err)

	}
	r.logger.Infof("install remote snapshot")
	resp.Success = true
	r.setLastContact()
}

// processVote 处理投票信息
func (r *Raft) processVote(req *VoteRequest, cmd *CMD) {
	var (
		resp = &VoteResponse{
			Term:        r.getCurrentTerm(),
			VoteGranted: false,
		}
		leader = r.getLeaderInfo()
		err    error
	)

	defer func() {
		resp.RPCHeader = r.buildRPCHeader(err)
		cmd.Response <- resp
	}()

	if !canVote(r.configurations.latest, req.ID) {
		// 如果 peer 有分配过 id
		// 不在最新配置里拒绝
		// 不能投票拒绝
		return
	}

	if len(leader.ID) != 0 && leader.ID != req.ID {
		// 当前已经有一个 leader 了，拒绝
		return
	}

	if req.Term < r.getCurrentTerm() {
		return
	}

	if req.Term > r.getCurrentTerm() {
		// 如果是新一轮的选举，那么我们直接转换为 follower 在继续处理逻辑
		r.setCurrentTerm(req.Term)
		r.setFollower()
		resp.Term = req.Term
	}

	lastVoteTerm, err := r.kvStorage.GetUint64(keyLastVoteTerm)
	if err != nil {
		return
	}
	lastVoteFor, err := r.kvStorage.Get(keyLastVoteCandidate)
	if err != nil {
		return
	}
	lastTerm, lastIndex := r.getLastLogEntry()
	if lastVoteTerm == req.Term && len(lastVoteFor) > 0 && ServerID(lastVoteFor) == req.ID {
		// 同一任期的同一个候选者可以重复投票
		resp.VoteGranted = true
		return
	}
	if lastTerm > req.Term {
		// 如果是前几个轮次的投票直接拒绝
		return
	}
	if lastTerm == req.LastLogTerm && lastIndex > req.LastLogIndex {
		// 如果是相同轮次的并且，当前最新的所以比远端大直接拒绝
		return
	}
	if r.persistVote(req.Term, req.Addr) != nil { // 将最新信息持久化
		return
	}

	resp.VoteGranted = true
	// 更新最新的联系时间，让 follower 继续等待
	r.setLastContact()
}

// processFollowerCmd 只处理 Follower 相关的信息
func (r *Raft) processCMD(cmd *CMD) {
	switch req := cmd.Request.(type) {
	case *VoteRequest:
		r.processVote(req, cmd)
	case *AppendEntryRequest:
		r.appendEntry(req, cmd)
	case *FastTimeOutRequest:
		r.fastTimeout(req, cmd)
	case *InstallSnapshotRequest:
		r.installSnapshot(req, cmd)
	}
}
func HasExistStage(logs LogStore, stable KVStorage,
	snaps SnapShotStore) (error, bool) {
	for _, f := range []func() (uint64, error){
		func() (uint64, error) {
			return stable.GetUint64(keyCurrentTerm)
		},
		func() (uint64, error) {
			return logs.LastIndex()
		},
		func() (uint64, error) {
			snapshotList, err := snaps.List()
			return uint64(len(snapshotList)), err
		},
	} {
		if t, err := f(); err != nil {
			return err, false
		} else if t > 0 {
			return nil, true
		}
	}

	return nil, false
}
func BootstrapCluster(conf *Conf, logs LogStore, stable KVStorage,
	snaps SnapShotStore, rpc RpcInterface, configuration configuration) error {
	if e := validateConf(conf); e != nil {
		return e
	}
	if err := checkConfiguration(configuration); err != nil {
		return err
	}
	err, has := HasExistStage(logs, stable, snaps)
	if err != nil {
		return err
	}
	if has {
		return ErrCantBootstrap
	}

	if err = stable.SetUint64(keyCurrentTerm, 1); err != nil {
		return err
	}
	entry := &LogEntry{
		Term:  1,
		Data:  EncodeConfiguration(configuration),
		Index: 1,
		Type:  LogConfiguration,
	}
	if err := logs.SetLogs([]*LogEntry{entry}); err != nil {
		return err
	}
	return nil
}

func (r *Raft) bootstrap(c configuration) error {
	if !canVote(c, r.localInfo.ID) {
		return ErrNotVoter
	}
	err := BootstrapCluster(r.Config(), r.logStore, r.kvStorage, r.snapShotStore, r.rpc, c)
	if err != nil {
		return err
	}
	log, err := r.logStore.GetLog(1)
	if err != nil {
		return err
	}
	r.setCurrentTerm(log.Term)
	r.setLastLog(log.Term, log.Index)
	return r.processConfigurationLogEntry(log)
}
func (r *Raft) while(state State, do func() (shouldContinue bool)) {
	for state == r.state.GetState() && do() {
	}
}

// checkLeadership 检查当前是否还有领导权
func (r *Raft) checkLeadership() (keep bool, maxDiff time.Duration) {
	var (
		contacted         int
		now               = time.Now()
		replState         = r.leaderState.replState
		leadershipTimeout = r.Config().LeadershipTimeout
	)
	for _, server := range r.getLatestServers() {
		if server.ID == r.localInfo.ID {
			contacted++
			continue
		}
		repl, ok := replState[server.ID]
		if !ok {
			continue
		}
		sub := now.Sub(repl.lastContact.Get())
		if sub > leadershipTimeout {
			continue
		}
		contacted++
		maxDiff = Max(maxDiff, sub)
	}
	return contacted >= r.quorumSize(), maxDiff
}
func canVote(c configuration, id ServerID) bool {
	for _, server := range c.Servers {
		if server.ID == id {
			return server.Suffrage == Voter
		}
	}
	return false
}
func (r *Raft) cycleFollower() {
	var (
		heartBeatCheckCh = randomTimeout(r.Config().HeartBeatTimeout)
		warnOnce         = sync.Once{}
		warn             = func(v ...any) {
			warnOnce.Do(func() {
				log.Println(v...)
			})
		}
	)

	runFollower := func() (shouldContinue bool) {
		select {
		case <-r.shutDownCh():
			return
		case <-r.leaderNotifyCh:
			// ignore
		case cmd := <-r.cmdChan:
			r.processCMD(cmd)
		case c := <-r.configurationChangeCh:
			c.fail(ErrNotLeader)
		case a := <-r.applyCh:
			a.fail(ErrNotLeader)
		case v := <-r.verifyCh:
			v.fail(ErrNotLeader)
		case u := <-r.userRestoreCh:
			u.fail(ErrNotLeader)
		case l := <-r.leadershipTransferCh:
			l.fail(ErrNotLeader)
		case c := <-r.configurationsGetCh:
			c.responded(r.configurations.Clone(), nil)
		case <-r.followerNotifyCh:
			// 变更心跳超时时间
			heartBeatCheckCh = time.After(0)
		case b := <-r.bootstrapCh:
			b.fail(r.bootstrap(b.configuration))
		case <-heartBeatCheckCh:
			heartBeatCheckCh = randomTimeout(r.Config().HeartBeatTimeout)
			// 如果未超时，则继续循环
			if time.Now().Sub(r.LastContact()) < r.Config().HeartBeatTimeout {
				return true
			}
			config := r.configurations
			oldLeaderInfo := r.getLeaderInfo()
			// 如果超时，及时不参加选举，也需要清理下上下文相关的字段
			r.clearLeaderInfo()
			switch {
			case config.latestIndex == 0:
				warn("unknown peers, aborting election")
				// 刚加入集群，不知道配置，放弃选举
			case config.latestIndex == config.commitIndex && !canVote(config.latest, r.localInfo.ID):
				warn("no part of stable configuration, aborting election")
				// 没有选举权，放弃选举
			case canVote(config.latest, r.localInfo.ID):
				warn("heartbeat abortCh reached, starting election", "last-leader-addr", oldLeaderInfo.Addr, "last-leader-id", oldLeaderInfo.ID)
				// 发起选举
				r.setCandidate()
			default:
				warn("heartbeat abortCh reached, not part of a stable configuration or a non-voter, not triggering a leader election")
			}
			// 继续循环
		}
		return true
	}
	r.while(Follower, runFollower)
}
func (r *Raft) buildAppendEntriesReq(fr *followerReplication, followerNextIndex, leaderLastIndex uint64) (*AppendEntryRequest, error) {
	req := &AppendEntryRequest{
		RPCHeader:    r.buildRPCHeader(nil),
		Term:         fr.term,
		LeaderCommit: r.commitIndex,
	}
	setEntries := func() error {
		maxAppendEntries := uint64(r.Config().MaxAppendEntries)
		maxIndex := Min(followerNextIndex+maxAppendEntries-1, leaderLastIndex)
		logs, err := r.logStore.GetLogRange(followerNextIndex, maxIndex)
		req.Entries = append(req.Entries, logs...)
		return err
	}
	setPreviousLog := func() error {

		return nil
	}
	for _, f := range []func() error{setEntries, setPreviousLog} {
		if err := f(); err != nil {
			return nil, err
		}
	}
	return req, nil
}
func (r *Raft) sendLatestSnapshot(fr *followerReplication) (err error) {
	snapShots, err := r.snapShotStore.List()
	if err != nil {
		return err
	}
	if len(snapShots) == 0 {
		return fmt.Errorf("no snapshots found")
	}
	snapID := snapShots[0].ID
	meta, snapshot, err := r.snapShotStore.Open(snapID)
	defer snapshot.Close()
	peer := fr.server.Get()

	req := &InstallSnapshotRequest{
		RPCHeader:          r.buildRPCHeader(nil),
		SnapShotVersion:    meta.Version,
		Term:               r.currentTerm,
		Size:               meta.Size,
		ConfigurationIndex: meta.configurationIndex,
		Configuration:      EncodeConfiguration(meta.configuration),
		Leader:             r.rpc.EncodeAddr(Ptr(r.getLeaderInfo())),
	}
	req.LastLogIndex, req.LastLogTerm = r.getLastLog()
	resp, err := r.rpc.InstallSnapShot(&peer, req, snapshot)
	if resp.Term < r.currentTerm {
		// 下台
		return nil
	}
	fr.lastContact.Set(time.Now())
	if resp.Success {
		// 更新索引
		fr.nextIndex = meta.Index + 1
		r.leaderState.commitment.match(peer.ID, meta.Index)
		// 重置失败数
		fr.failures = 0
	} else {
		fr.failures++
	}
	return nil
}
func (r *Raft) replicateTo(fr *followerReplication, lastIndex uint64) (stop bool) {
	checkMore := func() (more bool) {
		select {
		case <-fr.stopCh:
			return false
		default:
			if fr.nextIndex > lastIndex {
				return false
			}
		}
		return true
	}
	for checkMore() {
		req, err := r.buildAppendEntriesReq(fr, fr.nextIndex, lastIndex)
		if err != nil {
			if err == ErrNotFoundLog {
				r.sendLatestSnapshot(fr)
			} else {
				return false
			}
		}
		peer := fr.server.Get()
		resp, err := r.rpc.AppendEntries(&peer, req)
		if err != nil {
			fr.failures++
			return
		}
		if resp.Term > r.currentTerm {
			// 切换成follower
			asyncNotify(fr.stepDownCh)
			return true
		}
		// 更新最近一次的接触记录
		fr.lastContact.Set(time.Now())
		if resp.Success {
			fr.failures = 0
			fr.allowPipeline = true
		} else {
			// 更新索引位置
		}
	}

	return false

}
func (r *Raft) pipelineReplicateTo(fr *followerReplication) error {
	peer := fr.server.Get()
	pipeline, err := r.rpc.AppendEntryPipeline(&peer)
	if err != nil {
		return err
	}
	defer pipeline.Close()

	return nil

}
func (r *Raft) replicate(fr *followerReplication) {
	defer func() { close(fr.closeHeartbeatCh) }()
	var (
		shouldStop  bool
		replicateTo = func(lastLogIndex ...uint64) {
			if len(lastLogIndex) <= 0 {
				lastLogIndex = append(lastLogIndex, r.getLastIndex())
			}
			if len(lastLogIndex) <= 0 && lastLogIndex[0] <= 0 {
				shouldStop = true
				return
			}
			shouldStop = r.replicateTo(fr, lastLogIndex[0])
		}
	)

	for !shouldStop {
		select {
		case lastIndex := <-fr.stopCh:
			replicateTo(lastIndex)
		case deferResp := <-fr.triggerDeferRespCh:
			replicateTo()
			if shouldStop {
				deferResp.fail(fmt.Errorf("replicatoin failed"))
			} else {
				deferResp.success()
			}
		case <-fr.triggerCh:
			replicateTo()
		case <-randomTimeout(r.Config().CommitTimeout):
			replicateTo()
		}
		if shouldStop || !fr.allowPipeline {
			continue
		}

		if err := r.pipelineReplicateTo(fr); err != nil {
			if err != ErrPipelineReplicationNotSupported {
				log.Printf("failed to start pipeline replication to peer :%v ,error :%s", fr.server.Get(), err)
			}
		}
	}

}
func (r *Raft) heartbeat(fr *followerReplication) {
	server := fr.server.Get()
	req := &AppendEntryRequest{
		RPCHeader: r.buildRPCHeader(nil),
		Term:      fr.term,
		LeaderID:  r.getLeaderInfo().ID,
	}
	for {
		select {
		case <-randomTimeout(r.Config().HeartBeatTimeout):
		case <-fr.notifyCh:
		case <-fr.closeHeartbeatCh:
			return
		}
		resp, err := r.rpc.AppendEntries(&server, req)
		if err != nil {
			continue
		}
		fr.setLastContact()
		fr.notifyAll(resp.Success)
	}
}

func (r *Raft) persistVote(term uint64, addr ServerAddr) (err error) {
	if err = r.kvStorage.SetUint64(keyLastVoteTerm, term); err != nil {
		return
	}

	if err = r.kvStorage.Set(keyLastVoteCandidate, string(addr)); err != nil {
		return
	}
	return
}

// launchElection 给自己投一票，然后向其他节点发起选举请求
func (r *Raft) launchElection() chan *voteResult {
	var (
		lastTerm, lastIndex = r.getLastLogEntry()
		req                 = &VoteRequest{
			RPCHeader:          r.buildRPCHeader(nil),
			Term:               r.getCurrentTerm(),
			LastLogIndex:       lastIndex,
			LastLogTerm:        lastTerm,
			LeadershipTransfer: r.candidateFromLeadershipTransfer,
		}
		respChan = make(chan *voteResult, r.memberCount())
	)
	// 首先增加任期号
	r.setCurrentTerm(r.getCurrentTerm() + 1)

	for _, server := range r.getLatestServers() {
		switch {
		case server.Suffrage != Voter:
		case server.ID == r.localInfo.ID: // 选自己
			if err := r.persistVote(req.Term, r.localInfo.Addr); err != nil {
				return nil
			}
			respChan <- &voteResult{
				VoteResponse: &VoteResponse{
					Term:        r.currentTerm,
					VoteGranted: true,
				},
				ServerID: r.localInfo.ID,
			}
		default:
			r.goFunc(func() {
				resp, err := r.rpc.VoteRequest(&server, req)
				if err != nil {
					r.logger.Errorf("launchElection err :%s , peer :%+v", err, server)
					return
				}
				respChan <- &voteResult{
					ServerID:     server.ID,
					VoteResponse: resp,
				}
			})
		}
	}
	return respChan
}

func (r *Raft) getLatestServers() []ServerInfo {
	return r.configurations.latest.Servers
}

// quorumSize 获取投票获胜的法定人数
func (r *Raft) quorumSize() int {
	var voters int
	for _, server := range r.getLatestServers() {
		if server.Suffrage == Voter {
			voters++
		}
	}
	return voters<<1 + 1
}

// memberCount 获取当前集群成员信息，包括自己
func (r *Raft) memberCount() int {
	return len(r.getLatestServers())
}

func (r *Raft) setFollower() {
	r.state.setState(Follower)
	r.tick = r.cycleFollower
}
func (r *Raft) setShutDown() {
	r.state.setState(ShutDown)
	r.tick = func() {}
}
func (r *Raft) setCandidate() {
	r.state.setState(Candidate)
	r.tick = r.cycleCandidate
}
func (r *Raft) setLeader() {
	r.state.setState(Leader)
	r.updateLeaderInfo(func(s *ServerInfo) {
		*s = r.localInfo
	})
	r.tick = r.cycleLeader
}

func (r *Raft) cycleCandidate() {
	var (
		electionResultCh  = r.launchElection() // 开始选举
		grantedVotes      int
		quorumSize        = r.quorumSize()
		electionTimeout   = r.Config().ElectionTimeout
		electionTimeoutCh = randomTimeout(r.Config().ElectionTimeout)
	)

	runCandidate := func() (shouldContinue bool) {
		select {
		case <-r.shutDownCh():
			return
		case <-r.leaderNotifyCh:
			// ignore
		case cmd := <-r.cmdChan:
			r.processCMD(cmd)
		case c := <-r.configurationChangeCh:
			c.fail(ErrNotLeader)
		case a := <-r.applyCh:
			a.fail(ErrNotLeader)
		case v := <-r.verifyCh:
			v.fail(ErrNotLeader)
		case u := <-r.userRestoreCh:
			u.fail(ErrNotLeader)
		case l := <-r.leadershipTransferCh:
			l.fail(ErrNotLeader)
		case c := <-r.configurationsGetCh:
			c.responded(r.configurations.Clone(), nil)
		case b := <-r.bootstrapCh:
			b.fail(ErrCantBootstrap)
		case result := <-electionResultCh: // 接收选举结果
			if result.Term > r.getCurrentTerm() {
				r.logger.Debug("newer term discovered, fallback to follower", "term", result.Term)
				r.setCurrentTerm(result.Term)
				r.setFollower()
				return
			}
			if result.VoteGranted {
				r.logger.Debug("vote granted", "from", result.ID, "term", result.Term, "tally", grantedVotes)
				grantedVotes++
			}
			// 选举成功
			if grantedVotes >= quorumSize {
				r.logger.Info("election won", "term", result.Term, "tally", grantedVotes)
				r.setLeader()
			}
		case <-r.followerNotifyCh:
			// 可能会更新选举超时
			if electionTimeout != r.Config().ElectionTimeout {
				electionTimeout = r.Config().ElectionTimeout
				electionTimeoutCh = randomTimeout(electionTimeout)
			}
		case <-electionTimeoutCh:
			// 选举超时失败，重试
			r.logger.Warnf("Election abortCh reached, restarting election")
		}
		return true

	}
	r.while(Candidate, runCandidate)

}

// reloadAllReplicate 新节点开始复制,已移除的节点停止复制
func (r *Raft) reloadAllReplicate() {
	var (
		lastIndex = r.LastIndex() + 1
		inConfig  = make(map[ServerID]bool, len(r.getLatestServers()))
	)
	for _, server := range r.getLatestServers() {
		if server.ID == r.localInfo.ID {
			continue
		}
		server := server
		fr, ok := r.leaderState.replState[server.ID]
		if ok {
			fr.server.Set(server)
			continue
		}
		inConfig[server.ID] = true
		fr = &followerReplication{
			term:               r.currentTerm,
			nextIndex:          lastIndex,
			stepDownCh:         r.leaderState.stepDown,
			server:             NewLockItem(server),
			lastContact:        NewLockItem[time.Time](),
			notify:             NewLockItem(notifyMap{}),
			stopCh:             make(chan uint64, 1),
			notifyCh:           make(chan struct{}),
			closeHeartbeatCh:   make(chan struct{}),
			triggerCh:          make(chan struct{}),
			triggerDeferRespCh: make(chan *defaultDeferResponse),
		}
		r.goFunc(
			func() {
				r.replicate(fr)
			},
			func() {
				r.heartbeat(fr)
			},
		)
	}

	// 如果节点已经被移除则需要停止其心跳
	for id, repl := range r.leaderState.replState {
		if inConfig[id] {
			continue
		}
		repl.close()
		delete(r.leaderState.replState, id)
	}
}

const minLeaderShipTimeout = 10 * time.Millisecond

func (r *Raft) dispatchLogs(applyLogs []*LogFuture) error {
	var (
		currentTerm = r.currentTerm
		lastIndex   = r.raftContext.lastApplied
		log         []*LogEntry
	)
	for _, applyLog := range applyLogs {
		lastIndex++
		applyLog.log.Term = currentTerm
		applyLog.log.Index = lastIndex
		applyLog.log.CreatedAt = time.Now()
		log = append(log, applyLog.log)
		r.leaderState.inflight.PushBack(applyLog)
	}
	if err := r.logStore.SetLogs(log); err != nil {
		for _, applyLog := range applyLogs {
			applyLog.fail(err)
		}
		return err
	}
	r.leaderState.commitment.match(r.localInfo.ID, lastIndex)
	r.setLastLog(currentTerm, lastIndex)
	for _, repl := range r.leaderState.replState {
		asyncNotify(repl.triggerCh)
	}
	return nil
}

func (r *Raft) initLeaderState() {
	commitCh := make(chan struct{}, 1)
	r.leaderState = &LeaderState{
		inflight:   list.New(),
		stepDown:   make(chan struct{}, 1),
		replState:  map[ServerID]*followerReplication{},
		commitCh:   commitCh,
		commitment: newCommitment(commitCh, r.configurations.latest, r.lastApplied+1),
		notify:     map[*verifyFuture]struct{}{},
	}
}

func (r *Raft) verifyLeader(v *verifyFuture) {
	v.votes = 1
	v.quorumSize = r.quorumSize()

	if v.quorumSize == 1 {
		v.success()
		return
	}
	for _, replication := range r.leaderState.replState {
		replication.notify.Set(map[*verifyFuture]struct{}{
			v: {},
		})
		asyncNotify(replication.notifyCh)
	}

}

func (r *Raft) configurationChangeChIfStable() chan *configurationChangeFuture {
	if r.configurations.latestIndex == r.configurations.commitIndex &&
		r.getCommitIndex() >= r.leaderState.commitment.startIndex {
		return r.configurationChangeCh
	}
	return nil
}
func (r *Raft) appendConfigurationEntry(c *configurationChangeFuture) {
	newConfiguration, err := clacNewConfiguration(r.configurations.latest, r.configurations.latestIndex, c.req)
	if err != nil {
		c.fail(err)
		return
	}

	c.log = &LogEntry{
		Data: EncodeConfiguration(newConfiguration),
		Type: LogConfiguration,
	}

	r.dispatchLogs([]*LogFuture{&c.LogFuture})
	r.setLatestConfiguration(newConfiguration, c.log.Index)
	r.leaderState.commitment.setConfiguration(newConfiguration)
	r.reloadAllReplicate()
}

func checkConfiguration(config configuration) error {
	var (
		idSet   = make(map[ServerID]bool, len(config.Servers))
		addrSet = make(map[ServerAddr]bool, len(config.Servers))
		err     = fmt.Errorf
		voter   int
	)
	for _, server := range config.Servers {
		if idSet[server.ID] {
			return err("id conflict :%s", server.ID)
		}
		if addrSet[server.Addr] {
			return err("addr conflict :%s", server.Addr)
		}
		idSet[server.ID] = true
		addrSet[server.Addr] = true
		if server.Suffrage == Voter {
			voter++
		}
	}
	if voter == 0 {
		return err("no valid voters")
	}
	return nil
}

// clacNewConfiguration 计算最新的配置
func clacNewConfiguration(current configuration, currentIndex uint64, req configurationChangeRequest) (configuration, error) {
	if req.pervIndex > 0 && req.pervIndex != currentIndex {
		return configuration{}, fmt.Errorf("configuration changed since %d ,(lastest is %d)", req.pervIndex, currentIndex)
	}
	config := current.Clone()

	switch req.command {
	case AddVoter:
		var found bool
		for i, server := range config.Servers {
			if server.ID != req.peer.ID {
				continue
			}
			found = true
			config.Servers[i] = req.peer
			if server.Suffrage != Voter {
				config.Servers[i].Suffrage = Voter
			}
			break
		}
		if !found {
			config.Servers = append(config.Servers, req.peer)
		}
	case AddNonVoter:
		var found bool
		for i, server := range config.Servers {
			if server.ID != req.peer.ID {
				continue
			}
			found = true
			config.Servers[i] = req.peer
			if server.Suffrage != NonVoter {
				config.Servers[i].Suffrage = NonVoter
			}
			break
		}
		if !found {
			config.Servers = append(config.Servers, req.peer)
		}
	case DemoteVoter:
		for i, server := range config.Servers {
			if server.ID == req.peer.ID {
				config.Servers[i].Suffrage = NonVoter
				break
			}
		}
	case removeServer:
		for i, server := range config.Servers {
			if server.ID == req.peer.ID {
				config.Servers = append(config.Servers[:i], config.Servers[i+1:]...)
				break
			}
		}
	}
	return config, checkConfiguration(config)
}
func logFuture2CommitTuple(fu *LogFuture) *commitTuple {
	switch fu.log.Type {
	case LogBarrier, LogCommand, LogConfiguration:
		return &commitTuple{
			log:    fu.log,
			future: fu,
		}
	default:
		return nil
	}
}

func (r *Raft) processLogs(index uint64, futures map[uint64]*LogFuture) {

	lastApplied := r.getLastApplied()
	if index <= lastApplied {
		return
	}

	tupleList := []*commitTuple(nil)
	applyBatch := func(tupleList []*commitTuple) {
		select {
		case r.fsmMutateCh <- tupleList:
		case <-r.shutDownCh():
			for _, future := range tupleList {
				future.future.fail(ErrShutDown)
			}
		}
	}

	for idx := lastApplied + 1; idx < index; idx++ {
		var prepareLog *commitTuple
		lf, ok := futures[idx]
		if ok {
			prepareLog = logFuture2CommitTuple(lf)
		} else {
			logEntry, err := r.logStore.GetLog(index)
			if err != nil {
				return
			}
			prepareLog = &commitTuple{
				log: logEntry,
			}
		}
		switch {
		case prepareLog != nil:
			tupleList = append(tupleList, prepareLog)
			if len(tupleList) >= r.Config().MaxAppendEntries {
				applyBatch(tupleList)
			}
			tupleList = make([]*commitTuple, 0, r.Config().MaxAppendEntries)
		case ok:
			lf.success()
		}

	}
}
func (r *Raft) fastTimeoutWithPeer(info ServerInfo, repl *followerReplication, stopCh chan struct{}, doneCh chan error) {

	select {
	case <-stopCh:
		doneCh <- nil
		return
	default:
	}
	// 必须等到节点的进度跟上当前 leader 才可以正式发起切换
	for atomic.LoadUint64(&repl.nextIndex) <= r.getLastIndex() {
		resp := new(deferResponse[nilRespFuture])
		resp.init()
		repl.triggerDeferRespCh <- resp
		select {
		case err := <-resp.errCh:
			if err != nil {
				doneCh <- err
				return
			}
		case <-stopCh:
			doneCh <- nil
			return
		}
	}
	_, err := r.rpc.FastTimeOut(&info, &FastTimeOutRequest{r.buildRPCHeader(nil)})
	if err != nil {
		err = fmt.Errorf("failed to make FastTimeOut RPC to id :%s ,addr :%s", info.ID, info.Addr)
	}
	doneCh <- err
}

// pickLatestServer 领导人可以调用这个方法选择，当前集群中除领导人外具有最新日志的节点
func (r *Raft) pickLatestServer() (pick *ServerInfo) {
	var current uint64
	for _, info := range r.getLatestServers() {
		if info.ID == r.localInfo.ID || info.Suffrage != Voter {
			continue
		}
		state, ok := r.leaderState.replState[info.ID]
		if !ok {
			continue
		}
		nextIndex := atomic.LoadUint64(&state.nextIndex)
		if nextIndex > current {
			current = nextIndex
			info := info
			pick = &info
		}
	}
	return
}
func (r *Raft) cycleLeader() {
	var (
		leadershipTimeout = r.Config().LeadershipTimeout
		stepDown          bool // 用于标识 leader 是否已经下台，因为 select 会出现竞争，所以我们需要额外的同步标记
		leaveLeaderLoop   = make(chan struct{})
	)
	overrideNotifyBool(r.leaderCh, true)
	r.initLeaderState()
	defer func() {
		close(leaveLeaderLoop)
		r.setLastContact()
		r.leaderState.close()
		r.updateLeaderInfo(func(s *ServerInfo) {
			if s.Addr == r.localInfo.Addr && s.ID == r.localInfo.ID {
				*s = ServerInfo{}
			}
		})
	}()

	r.reloadAllReplicate()

	// 成为领导者后首先提交一条日志，成功后代表之前的日志均已提交
	if err := r.dispatchLogs([]*LogFuture{{log: &LogEntry{Type: LogNoop}}}); err != nil {
		// 提交失败则回退成 follower
		r.setFollower()
		return
	}

	var (
		leaderShipCh = time.After(leadershipTimeout)
	)
	runLeader := func() (shouldContinue bool) {
		select {
		case <-r.shutDownCh():
			// shutdown
			return
		case <-r.leaderNotifyCh:
			for _, replication := range r.leaderState.replState {
				asyncNotify(replication.notifyCh)
			}
		case <-r.followerNotifyCh:
			// 忽略
		case cmd := <-r.cmdChan:
			r.processCMD(cmd)
		case l := <-r.leadershipTransferCh:
			r.leadershipTransfer(l, leaveLeaderLoop)
		case verify := <-r.verifyCh:
			r.leaderVerify(verify)
		case logFuture := <-r.applyCh: // 日志提交
			r.logApply(logFuture, stepDown)
		case <-r.leaderState.stepDown:
			r.setFollower()
		case <-r.leaderState.commitCh:
			oldCommitIndex := r.commitIndex
			commitIndex := r.leaderState.commitment.GetCommitIndex()
			r.setCommitIndex(commitIndex)
			if r.configurations.latestIndex > oldCommitIndex && r.configurations.latestIndex < commitIndex {
				r.setLatestConfiguration(r.configurations.latest, r.configurations.latestIndex)
				if !canVote(r.configurations.latest, r.localInfo.ID) {
					stepDown = true
				}
			}
			var (
				groupFutures = map[uint64]*LogFuture{}
			)
			for e := r.leaderState.inflight.Front(); e != nil; e.Next() {
				lf := e.Value.(*LogFuture)
				if lf.log.Index > commitIndex {
					break
				}
				groupFutures[lf.log.Index] = lf
				r.leaderState.inflight.Remove(e)
			}
			r.processLogs(commitIndex, groupFutures)
			if !stepDown {
				return true
			}
			if r.Config().ShutdownOnRemove {
				r.ShutDown()
			} else {
				r.setFollower()
			}
		case <-leaderShipCh:
			if keep, maxDiff := r.checkLeadership(); keep {
				leaderShipCh = time.After(Max(leadershipTimeout-maxDiff, minLeaderShipTimeout))
			} else {
				r.setFollower()
				return
			}
		case c := <-r.configurationsGetCh:
			if r.leadershipTransferInProgress() {
				c.fail(ErrLeadershipTransferInProgress)
			} else {
				c.responded(r.configurations.Clone(), nil)
			}
		case c := <-r.configurationChangeChIfStable():
			if r.leadershipTransferInProgress() {
				c.fail(ErrLeadershipTransferInProgress)
			} else {
				r.appendConfigurationEntry(c)
			}
		case u := <-r.userRestoreCh:
			if r.leadershipTransferInProgress() {
				u.fail(ErrLeadershipTransferInProgress)
			} else {
				u.responded(nil, r.restoreUserSnapshot(u.meta, u.reader))
			}
		}
		return true
	}
	r.while(Leader, runLeader)
}

func (r *Raft) logApply(logFuture *LogFuture, stepDown bool) {
	accumulate := func(log *LogFuture) (entries []*LogFuture) {
		entries = []*LogFuture{log}
		for i := 0; i < r.Config().MaxAppendEntries; i++ {
			select {
			case logFuture := <-r.applyCh:
				entries = append(entries, logFuture)
			default:
				return
			}
		}
		return
	}
	if r.leadershipTransferInProgress() {
		r.logger.Debug("failed to apply log cause leadership transfer")
		logFuture.fail(ErrLeadershipTransferInProgress)
		return
	}
	entries := accumulate(logFuture)

	if !stepDown {
		r.dispatchLogs(entries)
		return
	}
	r.logger.Debug("failed to apply log cause step donw")
	for _, entry := range entries {
		entry.responded(nil, FutureErrNotLeader)
	}
}

func (r *Raft) leaderVerify(verify *verifyFuture) {
	switch {
	case verify.quorumSize == 0:
		//默认情况
		r.verifyLeader(verify)
	case verify.votes < verify.quorumSize:
		r.setFollower()
		delete(r.leaderState.notify, verify)
		for _, replication := range r.leaderState.replState {
			replication.notify.Action(func(t *notifyMap) {
				if t == nil {
					return
				}
				delete(*t, verify)
			})
		}
		verify.fail(ErrNotLeader)
	default:
		delete(r.leaderState.notify, verify)
		for _, replication := range r.leaderState.replState {
			replication.notify.Action(func(t *notifyMap) {
				if t == nil {
					return
				}
				delete(*t, verify)
			})
		}
		verify.success()
	}
}

// leadershipTransfer  领导权转移，可以指定某个节点立即开始新一轮选举，如果不指定默认选择当前具有最新日志的节点
//		     领导权将在指定节点获得选举权后自动转移
func (r *Raft) leadershipTransfer(l *leadershipTransferFuture, leaveLeaderLoop chan struct{}) {
	var (
		stopCh = make(chan struct{})
		doneCh = make(chan error)
	)
	if r.leadershipTransferInProgress() {
		r.logger.Debug(ErrLeadershipTransferInProgress.Error())
		l.fail(ErrLeadershipTransferInProgress)
		return
	}

	r.logger.Debug("starting leadership transfer , id :%s ,addr :%d", l.ServerInfo.ID, l.ServerInfo.Addr)

	if len(l.ServerInfo.ID) == 0 {
		if latest := r.pickLatestServer(); latest != nil {
			l.ServerInfo = *latest
		} else {
			l.fail(ErrLeadershipTransferFail)
			return
			// 失败，没找到合适的节点
		}
	}

	go func() {
		defer r.setLeadershipTransferInProgress(false)
		select {
		case <-time.After(r.Config().ElectionTimeout):
			close(stopCh)
			err := errors.New("leadership transfer timeout")
			r.logger.Debug(err.Error())
			l.fail(err)
		case <-leaveLeaderLoop:
			close(stopCh)
			r.logger.Debug("lost leadership during transfer (expected)")
			l.success()
		case err := <-doneCh:
			if err != nil {
				r.logger.Debug(err.Error())
			}
			l.responded(nil, err)
			return
		}
		<-doneCh
	}()

	state, ok := r.leaderState.replState[l.ServerInfo.ID]
	if !ok {
		doneCh <- fmt.Errorf("connot find replication state for :%s", l.ServerInfo.ID)
		return
	}
	r.setLeadershipTransferInProgress(true)
	go r.fastTimeoutWithPeer(l.ServerInfo, state, stopCh, doneCh)
}
