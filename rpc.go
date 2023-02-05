package go_raft

import (
	"io"
	"time"
)

type (
	CMD struct {
		CmdType  uint8
		Request  interface{}
		Reader   io.Reader // 安装快照的时候用
		Response chan interface{}
	}
	RpcInterface interface {
		VoteRequest(*ServerInfo, *VoteRequest) (*VoteResponse, error)
		AppendEntries(*ServerInfo, *AppendEntryRequest) (*AppendEntryResponse, error)
		AppendEntryPipeline(*ServerInfo) (AppendEntryPipeline, error)
		InstallSnapShot(*ServerInfo, *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
		// SetFastPath 用于快速处理，不用经过主流程，不支持也没关系
		FastPath
	}
	FastPath interface {
		SetFastPath(cb fastPath)
	}

	AppendEntryPipeline interface {
		AppendEntries(*AppendEntryRequest) (AppendEntriesFuture, error)
		Consumer() chan AppendEntriesFuture
		Close() error
	}
)

func (s *NetTransport) SetFastPath(cb fastPath) {
	if fp, ok := s.processor.(FastPath); ok {
		fp.SetFastPath(cb)
	}
}

func (s *NetTransport) VoteRequest(info *ServerInfo, request *VoteRequest) (*VoteResponse, error) {
	resp := new(VoteResponse)
	err := s.Dial(info, CmdVoteRequest, request, resp)
	return resp, err
}

func (s *NetTransport) AppendEntries(info *ServerInfo, request *AppendEntryRequest) (*AppendEntryResponse, error) {
	resp := new(AppendEntryResponse)
	err := s.Dial(info, CmdAppendEntry, request, resp)
	return resp, err
}

func (s *NetTransport) AppendEntryPipeline(peer *ServerInfo) (AppendEntryPipeline, error) {
	conn, err := s.client.GetConnection(peer, time.Second)
	if err != nil {
		return nil, err
	}
	return newNetPipeline(s, conn), err
}

func (s *NetTransport) InstallSnapShot(info *ServerInfo, request *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	resp := new(InstallSnapshotResponse)
	err := s.Dial(info, CmdInstallSnapshot, request, resp)
	return nil, err
}
