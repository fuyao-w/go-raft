package go_raft

import (
	"io"
	"sync"
	"time"
)

type memRPC struct {
	sync.Mutex
	consumerCh chan *CMD
	localAddr  ServerAddr
	peerMap    map[ServerAddr]*memRPC
	pipeline   []*memPipeline
	timeout    time.Duration
}

func newMemRpc() *memRPC {
	return &memRPC{}
}
func (m *memRPC) getPeer(addr ServerAddr) *memRPC {
	m.Lock()
	defer m.Unlock()
	_, ok := m.peerMap[addr]
	if !ok {
		m.peerMap[addr] = newMemRpc()
	}
	return m.peerMap[addr]
}

type memPipeline struct {
}

func (m *memRPC) Consumer() <-chan *CMD {
	return m.consumerCh
}

func (m *memRPC) VoteRequest(info *ServerInfo, request *VoteRequest) (*VoteResponse, error) {
	//peer := m.getPeer(info.Addr)
	panic("implement me")
}

func (m *memRPC) AppendEntries(info *ServerInfo, request *AppendEntryRequest) (*AppendEntryResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *memRPC) AppendEntryPipeline(info *ServerInfo) (AppendEntryPipeline, error) {
	//TODO implement me
	panic("implement me")
}

func (m *memRPC) InstallSnapShot(info *ServerInfo, request *InstallSnapshotRequest, reader io.Reader) (*InstallSnapshotResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *memRPC) SetHeartbeatFastPath(cb fastPath) {
	//TODO implement me
	panic("implement me")
}

func (m *memRPC) FastTimeOut(info *ServerInfo, req *FastTimeOutReq) (*FastTimeOutResp, error) {
	//TODO implement me
	panic("implement me")
}

func (m *memRPC) LocalAddr() ServerAddr {
	//TODO implement me
	panic("implement me")
}

func (m *memRPC) EncodeAddr(info *ServerInfo) []byte {
	//TODO implement me
	panic("implement me")
}

func (m *memRPC) DecodeAddr(bytes []byte) ServerAddr {
	//TODO implement me
	panic("implement me")
}
