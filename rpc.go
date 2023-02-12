package go_raft

import (
	"io"
)

type (
	cmdType uint8
	CMD     struct {
		CmdType  cmdType
		Request  any
		Reader   io.Reader // 安装快照的时候用
		Response chan any
	}
	RpcInterface interface {
		// Consumer 返回一个可消费的 Chan
		Consumer() <-chan *CMD
		// VoteRequest 发起投票请求
		VoteRequest(*ServerInfo, *VoteRequest) (*VoteResponse, error)
		// AppendEntries 追加日志
		AppendEntries(*ServerInfo, *AppendEntryRequest) (*AppendEntryResponse, error)
		// AppendEntryPipeline 以 pipe 形式追加日志
		AppendEntryPipeline(*ServerInfo) (AppendEntryPipeline, error)
		// InstallSnapShot 安装快照
		InstallSnapShot(*ServerInfo, *InstallSnapshotRequest, io.Reader) (*InstallSnapshotResponse, error)
		// SetHeartbeatFastPath 用于快速处理，不用经过主流程，不支持也没关系
		SetHeartbeatFastPath(cb fastPath)
		// FastTimeOut 快速超时转换为候选人
		FastTimeOut(*ServerInfo, *FastTimeOutReq) (*FastTimeOutResp, error)

		LocalAddr() ServerAddr
		EncodeAddr(info *ServerInfo) []byte
		DecodeAddr([]byte) ServerAddr
	}

	AppendEntryPipeline interface {
		AppendEntries(*AppendEntryRequest) (AppendEntriesFuture, error)
		Consumer() <-chan AppendEntriesFuture
		Close() error
	}
)
