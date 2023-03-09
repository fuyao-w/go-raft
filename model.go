package go_raft

type (
	// AppendEntryRequest 追加日志
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
		Term         uint64 `json:"term"`           // 响应者的当前任期，对于领导人而言 它会更新自己的任期
		LastLogIndex uint64 `json:"last_log_index"` // follower 最新的日志 index
		Success      bool   `json:"success"`        // 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
	}
	// VoteRequest 投票
	VoteRequest struct {
		*RPCHeader
		Term               uint64 `json:"term"`                // 节点当前任期
		LastLogIndex       uint64 `json:"last_log_index"`      // 节点最新日志的索引
		LastLogTerm        uint64 `json:"last_log_term"`       // 节点最新日志的任期
		LeadershipTransfer bool   `json:"leadership_transfer"` // 是否由主动引导 leader 切换导致的选举
	}
	VoteResponse struct {
		*RPCHeader
		Term        uint64 `json:"term"`         // 响应者的当前任期，以便于候选人去更新自己的任期号
		VoteGranted bool   `json:"vote_granted"` // 候选人赢得了此张选票时为真
	}
	// InstallSnapshotRequest 安装快照
	InstallSnapshotRequest struct {
		*RPCHeader
		SnapShotVersion           // 快照版本，预留
		Term               uint64 `json:"term"` // leader 任期
		LastLogIndex       uint64 `json:"last_log_index"`
		LastLogTerm        uint64 `json:"last_log_term"`       // leader 当前最新的日志索引和任期
		Size               int64  `json:"size"`                // 快照大小
		ConfigurationIndex uint64 `json:"configuration_index"` // 配置信息的日志 index
		Configuration      []byte `json:"configuration"`       // 集群配置信息
		Leader             []byte `json:"leader"`              // 如果有人工引导，可以通过该字段指定 leader 的地址
	}
	InstallSnapshotResponse struct {
		*RPCHeader
		Success bool   `json:"success"`
		Term    uint64 `json:"term"`
	}
	// FastTimeOutRequest 引导 leader 直接超时
	FastTimeOutRequest struct {
	}
	FastTimeOutResponse struct {
	}
	// ServerInfo 节点信息
	ServerInfo struct {
		Suffrage ServerSuffrage // 该节点是否选举权
		ID       ServerID
		Addr     ServerAddr
	}
	// voteResult 投票结果
	voteResult struct {
		*VoteResponse
		ServerID ServerID
	}
	// ServerID 节点的 ID
	ServerID string
	// ServerAddr 节点地址
	ServerAddr string

	RPCHeader struct {
		ID     ServerID   `json:"id"`
		Addr   ServerAddr `json:"addr"`
		ErrMsg string     `json:"err_msg"`
	}
)
