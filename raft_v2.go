package go_raft

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
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
)

func (r *Raft) Run() {
	for {
		select {
		case <-r.shutDown.ch:
			r.leaderInfo.Set(ServerInfo{})
			return
		default:
		}
		r.tick()
	}
}

func (r *Raft) getState() State {
	return State(atomic.LoadUint64((*uint64)(&r.state)))
}
func (r *Raft) setState(newState State) {
	r.getLastLogEntry()
	atomic.StoreUint64((*uint64)(&r.state), (uint64)(newState))
}

func (r *Raft) tick() {
	switch r.getState() {
	case Follower:
		r.cycleFollower()
	case Candidate:
		r.cycleCandidate()
	case Leader:
		r.cycleLeader()
	case ShutDown:
	}
}

func (r *Raft) setCommittedConfiguration(c configuration, i uint64) {
	r.configurations.commit = c
	r.configurations.commitIndex = i
}
func (r *Raft) setLatestConfiguration(c configuration, i uint64) {
	r.configurations.latest = c
	r.configurations.commitIndex = i
	r.latestConfiguration.Store(c.Clone())
}

func (r *Raft) updateLeaderInfo(act func(s *ServerInfo)) {
	r.leaderInfo.Action(act)
}
func DecodeConfiguration(data []byte) (c configuration) {
	json.Unmarshal(data, &c)
	return
}
func EnecodeConfiguration(c configuration) (data []byte) {
	data, _ = json.Marshal(c)
	return
}
func (r *Raft) processConfigurationLogEntry(entry *LogEntry) error {
	switch entry.Type {
	case LogConfiguration:
		r.setCommittedConfiguration(r.configurations.latest, r.configurations.latestIndex)
		//
		r.setLatestConfiguration(DecodeConfiguration(entry.Data), entry.Index)
	}
	return nil
}

// processAppendEntries 处理心跳
func (r *Raft) processHeartBeat(req *AppendEntryRequest, cmd *CMD) {
	r.processAppendEntries(req, cmd)
}

// processAppendEntries 处理日志提交
func (r *Raft) processAppendEntries(req *AppendEntryRequest, cmd *CMD) {
	var (
		lastTerm, lastIndex = r.getLastLogEntry()
		resp                = &AppendEntryResponse{
			Term:    r.getCurrentTerm(),
			LastLog: lastIndex,
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
			idx := min(req.LeaderCommit, lastIndex)
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
	if r.persistVote(req.Term, string(req.ID)) != nil { // 将最新信息持久化
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
}

func (r *Raft) bootstrap(c configuration) error {
	return nil
}
func (r *Raft) while(state State, do func() (end bool)) {
	for state == r.state.GetState() && do() {
	}
}

// checkLeadership 检查当前是否还有领导权
func (r *Raft) checkLeadership() (bool, time.Duration) {

	var (
		contacted int
		now       = time.Now()
		replState = r.leaderState.replState
		maxDiff   time.Duration
		tm        = r.Config().LeaderShipTimeout
	)
	for _, server := range r.configurations.latest.servers {
		if server.ID == r.localAddr.ID {
			contacted++
			continue
		}
		repl, ok := replState[server.ID]
		if !ok {
			continue
		}
		sub := now.Sub(repl.lastContact.Get())
		if sub > tm {
			continue
		}
		contacted++
		maxDiff = max(maxDiff, sub)
	}
	return contacted >= r.quorumSize(), maxDiff
}
func canVote(c configuration, id ServerID) bool {
	for _, server := range c.servers {
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

	runFollower := func() (end bool) {
		select {
		case <-r.shutDown.ch:
			return true
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
		case c := <-r.configurationGetCh:
			c.responded(r.configurations.Clone(), nil)

		case <-r.followerNotifyCh:
			// 变更心跳超时时间
			heartBeatCheckCh = time.After(0)

		case b := <-r.bootstrapCh:
			b.fail(r.bootstrap(b.configuration))

		case <-heartBeatCheckCh:
			heartBeatCheckCh = randomTimeout(r.Config().HeartBeatTimeout)
			// 如果未超时，则继续循环
			if time.Now().Sub(r.lastContact.Get()) < r.Config().HeartBeatTimeout {
				return true
			}
			config := r.configurations
			oldLeaderInfo := r.getLeaderInfo()
			// 如果超时，及时不参加选举，也需要清理下上下文相关的字段
			r.clear()
			switch {
			case config.latestIndex == 0:
				warn("unknown peers ,aborting election")
				// 刚加入集群，不知道配置，放弃选举
			case config.latestIndex == config.commitIndex && !canVote(config.latest, r.localAddr.ID):
				warn("no part of stable configuration ,aborting election")
				// 没有选举权，放弃选举
			case canVote(config.latest, r.localAddr.ID):
				warn("heartbeat timeout reached, starting election", "last-leader-addr", oldLeaderInfo.Addr, "last-leader-id", oldLeaderInfo.ID)
				// 发起选举
				r.setCandidate()
			default:
				warn("heartbeat timeout reached, not part of a stable configuration or a non-voter, not triggering a leader election")
			}
		}
		return
	}
	r.while(Follower, runFollower)
}
func (r *Raft) buildAppendEntriesReq(fr *followerReplication, followerNextIndex, leaderLastIndex uint64) (*AppendEntryRequest, error) {
	req := &AppendEntryRequest{
		RPCHeader:    r.buildRPCHeader(nil),
		Term:         fr.Term,
		LeaderCommit: r.commitIndex,
	}
	setEntries := func() error {
		maxAppendEntries := uint64(r.Config().MaxAppendEntries)
		maxIndex := min(followerNextIndex+maxAppendEntries-1, leaderLastIndex)
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

	resp, err := r.rpc.InstallSnapShot(&peer, &InstallSnapshotRequest{
		// TODO 参数不全
	})
	if resp.Term < r.currentTerm {
		// 下台
		return nil
	}
	fr.lastContact.Set(time.Now())
	if resp.Success {
		// 更新索引
		fr.nextIndex = meta.Index + 1
		r.commitment.match(peer.ID, meta.Index)
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
	closeHeartBeatCh := make(chan struct{})
	r.goFunc(func() {
		r.heartbeat(fr, closeHeartBeatCh)
	})
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
func (r *Raft) heartbeat(fr *followerReplication, stopHeartBeatCh chan struct{}) {
	server := fr.server.Get()
	for {
		select {
		case <-stopHeartBeatCh:
			return
		case <-fr.stepDownCh:
			return
		case <-randomTimeout(r.Config().HeartBeatTimeout):
			resp, err := r.rpc.AppendEntries(&server, &AppendEntryRequest{
				Term:     fr.Term,
				LeaderID: r.getLeaderInfo().ID,
			})
			if err != nil {
				continue
			}
			if resp.Success {
				fr.lastContact.Set(time.Now())
			} else if resp.Term > r.currentTerm {
				r.setCurrentTerm(resp.Term)
				r.setFollower()
			}
		case <-fr.stopCh:
			return
		}

	}
}

func (r *Raft) persistVote(term uint64, addr string) (err error) {
	if err = r.kvStorage.SetUint64(keyLastVoteTerm, r.getCurrentTerm()); err != nil {
		return
	}

	if err = r.kvStorage.Set(keyLastVoteCandidate, addr); err != nil {
		return
	}
	return
}
func (r *Raft) election() chan *voteResult {
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

	ask := func(peer ServerInfo) {
		r.goFunc(func() {
			resp, err := r.rpc.VoteRequest(&peer, req)
			if err != nil {
				return
			}
			respChan <- &voteResult{
				ServerID:     peer.ID,
				VoteResponse: resp,
			}
		})
	}
	for _, server := range r.configurations.latest.servers {
		if server.Suffrage != Voter {
			continue
		}
		// 选自己
		if server.ID == r.localAddr.ID {
			if err := r.persistVote(req.Term, r.localAddr.Addr); err != nil {
				return nil
			}
			respChan <- &voteResult{
				VoteResponse: &VoteResponse{
					Term:        r.currentTerm,
					VoteGranted: true,
				},
				ServerID: r.localAddr.ID,
			}
		} else {
			ask(server)
		}
	}
	return respChan
}

func (r *Raft) getServers(latest bool) []ServerInfo {
	if latest {
		return r.configurations.latest.servers
	}
	return r.configurations.commit.servers
}

// quorumSize 获取投票获胜的法定人数
func (r *Raft) quorumSize() (voters int) {
	for _, server := range r.getServers(true) {
		if server.Suffrage == Voter {
			voters++
		}
	}
	return r.memberCount()<<1 + 1
}

// memberCount 获取当前集群成员信息，包括自己
func (r *Raft) memberCount() int {
	return len(r.getServers(true))
}

func (r *Raft) setFollower() {
	r.clear()
	r.state.setState(Follower)
}
func (r *Raft) setShutDown() {
	r.clear()
	r.state.setState(ShutDown)
}
func (r *Raft) setCandidate() {
	r.clear()
	r.state.setState(Candidate)
}
func (r *Raft) setLeader() {
	r.clear()
	r.state.setState(Leader)
	r.leaderInfo.Set(r.localAddr)
}

func (r *Raft) cycleCandidate() {
	var (
		electionResultCh  = r.election() // 开始选举
		grantedVotes      int
		quorumSize        = r.quorumSize()
		electionTimeout   = r.Config().ElectionTimeout
		electionTimeoutCh = randomTimeout(r.Config().ElectionTimeout)
	)

	runCandidate := func() (end bool) {
		select {
		case <-r.shutDown.ch:
			return true
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
			l.fail(ErrNotLeader)
		case c := <-r.configurationGetCh:
			c.responded(r.configurations.Clone(), nil)
		case b := <-r.bootstrapCh:
			b.fail(ErrCantBootstrap)
		case result := <-electionResultCh: // 接收选举结果
			if result.Term > r.getCurrentTerm() {
				r.setCurrentTerm(result.Term)
				r.setFollower()
				return
			}
			if result.VoteGranted {
				grantedVotes++
			}
			// 选举成功
			if grantedVotes >= quorumSize {
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
			return true
		}
		return

	}
	r.while(Candidate, runCandidate)

}

// startStopReplicate 对新节点开始心跳，如果节点变化过，则停止已移除 servers 的节点
func (r *Raft) startStopReplicate() {
	var inConfig = make(map[ServerID]bool, len(r.configurations.latest.servers))

	for _, server := range r.configurations.latest.servers {
		if server.ID == r.localAddr.ID {
			continue
		}
		server := server
		fr, ok := r.leaderState.replState[server.ID]
		switch {
		case ok:
			fr.server.Set(server)
		default:
			inConfig[server.ID] = true
			r.goFunc(func() {
				r.replicate(&followerReplication{
					Term: r.currentTerm,
					//nextIndex:   r.NextIndex,
					stepDownCh:  r.leaderState.stepDown,
					stopCh:      nil,
					server:      lockItem[ServerInfo]{item: server},
					lastContact: lockItem[time.Time]{},
				})
			})

		}
	}

	// 如果节点已经被移除则需要停止其心跳
	for id, repl := range r.leaderState.replState {
		if inConfig[id] {
			continue
		}
		close(repl.stopCh)

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
	r.leaderState.commitment.match(r.localAddr.ID, lastIndex)
	r.setLastLog(currentTerm, lastIndex)
	for _, repl := range r.leaderState.replState {
		asyncNotify(repl.triggerCh)
	}
	return nil
}

func (r *Raft) initLeaderState() {
	commitCh := make(chan struct{}, 1)
	r.leaderState = LeaderState{
		inflight:   list.New(),
		stepDown:   make(chan struct{}, 1),
		replState:  map[ServerID]*followerReplication{},
		commitCh:   commitCh,
		commitment: newCommitment(commitCh, r.configurations.latest, r.lastApplied+1),
		notify:     map[*verifyFuture]struct{}{},
	}
}

func (r *Raft) verifyLeader(v *verifyFuture) {
	v.votes++
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
		Data: EnecodeConfiguration(newConfiguration),
		Type: LogConfiguration,
	}

	r.dispatchLogs([]*LogFuture{&c.LogFuture})
	r.setLatestConfiguration(newConfiguration, c.log.Index)
	r.leaderState.commitment.setConfiguration(newConfiguration)
	r.startStopReplicate()
}

func checkConfiguration(config configuration) error {
	var (
		idSet   = make(map[ServerID]bool, len(config.servers))
		addrSet = make(map[string]bool, len(config.servers))
		err     = fmt.Errorf
		voter   int
	)
	for _, server := range config.servers {
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

	switch req.commend {
	case AddVoter:
		var found bool
		for i, server := range config.servers {
			if server.ID != req.peer.ID {
				continue
			}
			found = true
			config.servers[i] = req.peer
			if server.Suffrage != Voter {
				config.servers[i].Suffrage = Voter
			}
			break
		}
		if !found {
			config.servers = append(config.servers, req.peer)
		}
	case AddNonVoter:
		var found bool
		for i, server := range config.servers {
			if server.ID != req.peer.ID {
				continue
			}
			found = true
			config.servers[i] = req.peer
			if server.Suffrage != NonVoter {
				config.servers[i].Suffrage = NonVoter
			}
			break
		}
		if !found {
			config.servers = append(config.servers, req.peer)
		}
	case DemoteVoter:
		for i, server := range config.servers {
			if server.ID == req.peer.ID {
				config.servers[i].Suffrage = NonVoter
				break
			}
		}
	case removeServer:
		for i, server := range config.servers {
			if server.ID == req.peer.ID {
				config.servers = append(config.servers[:i], config.servers[i+1:]...)
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
		case <-r.shutDown.ch:
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
func (r *Raft) cycleLeader() {
	overrideNotifyBool(r.leaderCh, true)
	r.initLeaderState()
	r.startStopReplicate()
	var (
		lst = r.Config().LeaderShipTimeout

		stepDown   bool // 用于标识 leader 是否已经下台，因为 select 会出现竞争，所以我们需要额外的同步标记
		accumulate = func(log *LogFuture) (entries []*LogFuture) {
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
	)
	// 成为领导者后首先提交一条日志， TODO 原因是为啥来着，，，，
	if err := r.dispatchLogs([]*LogFuture{{log: &LogEntry{Type: LogNoop}}}); err != nil {
		// 提交失败则回退成 follower
		r.setFollower()
	}

	var (
		leaderShipCh = time.After(lst)
	)
	runLeader := func() (end bool) {
		select {
		case <-r.shutDown.ch:
			// shutdown
			return true
		case <-r.leaderNotifyCh:
			for _, replication := range r.leaderState.replState {
				asyncNotify(replication.notifyCh)
			}
		case <-r.followerNotifyCh:
			// 忽略
		case cmd := <-r.cmdChan:
			r.processCMD(cmd)
		case verify := <-r.verifyCh:
			switch {
			case verify.quorumSize == 0:
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
		case logFuture := <-r.applyCh: // 日志提交
			if r.leadershipTransferInProgress() {
				logFuture.fail(ErrLeadershipTransferInProgress)
				return
			}
			entries := accumulate(logFuture)

			if stepDown {
				for _, entry := range entries {
					entry.responded(nil, FutureErrNotLeader)
				}
				return
			}
			r.dispatchLogs(entries)
		case <-r.leaderState.stepDown:
			r.setFollower()
			return true
		case <-r.leaderState.commitCh:
			oldCommitIndex := r.commitIndex
			commitIndex := r.commitment.GetCommitIndex()
			r.setCommitIndex(commitIndex)
			if r.configurations.latestIndex > oldCommitIndex && r.configurations.latestIndex < commitIndex {
				r.setLatestConfiguration(r.configurations.latest, r.configurations.latestIndex)
				if !canVote(r.configurations.latest, r.localAddr.ID) {
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
				return
			}
			if r.Config().ShutdownOnRemove {
				r.ShutDown()
			} else {
				r.setFollower()
			}
		case <-leaderShipCh:
			if keep, maxDiff := r.checkLeadership(); keep {
				leaderShipCh = time.After(max(lst-maxDiff, minLeaderShipTimeout))
			} else {
				r.setFollower()

			}
		case c := <-r.configurationGetCh:
			if r.leadershipTransferInProgress() {
				c.fail(ErrLeadershipTransferInProgress)
				return
			}
			c.responded(r.configurations.Clone(), nil)
		case c := <-r.configurationChangeChIfStable():
			if r.leadershipTransferInProgress() {
				c.fail(ErrLeadershipTransferInProgress)
				return
			}
			r.appendConfigurationEntry(c)
		case u := <-r.userRestoreCh:
			if r.leadershipTransferInProgress() {
				u.fail(ErrLeadershipTransferInProgress)
				return
			}
			u.responded(nil, r.restoreUserSnapshot(u.meta, u.reader))
		}
		return false
	}
	r.while(Leader, runLeader)
}
