package go_raft

type (
	AppendEntryRequest struct {
		*RPCHeader
		Term         uint64      `json:"term"`           // 领导人的任期
		LeaderID     ServerID    `json:"leader_id"`      // 领导人 ID 因此跟随者可以对客户端进行重定向
		PrevLogIndex uint64      `json:"prev_log_index"` // 紧邻新日志条目之前的那个日志条目的索引
		PrevLogTerm  uint64      `json:"prev_log_term"`  // 紧邻新日志条目之前的那个日志条目的任期
		Entries      []*LogEntry `json:"entries"`        // 需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
		LeaderCommit uint64      `json:"leader_commit"`  // 领导人的已知己提交的最高的日志条目的索引
	}
	AppendEntryResponse struct {
		*RPCHeader
		Term    uint64 `json:"term"` // 响应者的当前任期，对于领导人而言 它会更新自己的任期
		LastLog uint64 `json:"last_log"`
		Success bool   `json:"success"` // 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
	}

	VoteRequest struct {
		*RPCHeader
		Term               uint64 `json:"term"`
		LastLogIndex       uint64 `json:"last_log_index"`
		LastLogTerm        uint64 `json:"last_log_term"`
		LeadershipTransfer bool   `json:"leadership_transfer"`
	}
	VoteResponse struct {
		*RPCHeader
		Term        uint64 `json:"term"`         // 响应者的当前任期，以便于候选人去更新自己的任期号
		VoteGranted bool   `json:"vote_granted"` // 候选人赢得了此张选票时为真
	}
	InstallSnapshotRequest struct {
		*RPCHeader
		SnapShotVersion
		Term                      uint64
		LastLogIndex, LastLogTerm uint64
		Size                      int64
		ConfigurationIndex        uint64
		Configuration             []byte
		Leader                    []byte
	}
	FastTimeOutReq struct {
	}
	FastTimeOutResp struct {
	}
	InstallSnapshotResponse struct {
		Success bool
		Term    uint64
	}
	ServerInfo struct {
		// Suffrage determines whether the server gets a election.
		Suffrage ServerSuffrage
		ID       ServerID
		Addr     ServerAddr
	}

	voteResult struct {
		*VoteResponse
		ServerID ServerID
	}

	ServerID   string
	ServerAddr string

	RPCHeader struct {
		ID     ServerID
		Addr   ServerAddr
		ErrMsg string
	}
)

func (r *Raft) clear() {

	r.updateLeaderInfo(func(s *ServerInfo) {
		*s = ServerInfo{}
	})
	//n.votedFor = ""
	//n.NextIndex = 0 TODO
	//n.MatchIndex = 0 TODO
}
func (n *raftContext) UpdateTerm() {
	n.currentTerm++
	//n.votedFor = ""
}
