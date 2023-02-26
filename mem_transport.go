package go_raft

import (
	"errors"
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
	processor  Processor
}

type menAppendEntryPipeline struct {
}

func (m *menAppendEntryPipeline) AppendEntries(request *AppendEntryRequest) (AppendEntriesFuture, error) {
	//TODO implement me
	panic("implement me")
}

func (m *menAppendEntryPipeline) Consumer() <-chan AppendEntriesFuture {

}

func (m *menAppendEntryPipeline) Close() error {
	return nil
}

func newMemRpc() *memRPC {
	cmdCh := make(chan *CMD)
	return &memRPC{
		consumerCh: cmdCh,
		processor:  newProcessorProxy(cmdCh),
		peerMap:    map[ServerAddr]*memRPC{},
		timeout:    time.Second,
	}
}
func (m *memRPC) getPeer(addr ServerAddr) *memRPC {
	m.Lock()
	defer m.Unlock()
	return m.peerMap[addr]
}

type memPipeline struct {
}

func (m *memRPC) Consumer() <-chan *CMD {
	return m.consumerCh
}
func (m *memRPC) doRpc(peer *memRPC, request interface{}) (interface{}, error) {
	timeout := m.timeout
	cmd := &CMD{
		CmdType:  0,
		Request:  request,
		Response: make(chan interface{}),
	}
	now := time.Now()
	select {
	case peer.consumerCh <- cmd:
		timeout = time.Now().Sub(now)
	case <-time.After(timeout):
	}

	select {
	case resp := <-cmd.Response:
		return resp, nil
	case <-time.After(m.timeout):
		return nil, errors.New("time out")
	}
}

func (m *memRPC) VoteRequest(info *ServerInfo, request *VoteRequest) (*VoteResponse, error) {
	resp, err := m.doRpc(m.getPeer(info.Addr), request)
	if err != nil {
		return nil, err
	}
	return resp.(*VoteResponse), nil
}

func (m *memRPC) AppendEntries(info *ServerInfo, request *AppendEntryRequest) (*AppendEntryResponse, error) {
	resp, err := m.doRpc(m.getPeer(info.Addr), request)
	if err != nil {
		return nil, err
	}
	return resp.(*AppendEntryResponse), nil
}

func (m *memRPC) AppendEntryPipeline(info *ServerInfo) (AppendEntryPipeline, error) {
	peer := m.getPeer(info.Addr)
	return &menAppendEntryPipeline{}, nil
}

func (m *memRPC) InstallSnapShot(info *ServerInfo, request *InstallSnapshotRequest, reader io.Reader) (*InstallSnapshotResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *memRPC) SetHeartbeatFastPath(cb fastPath) {
	m.processor.SetFastPath(cb)
}

func (m *memRPC) FastTimeOut(info *ServerInfo, request *FastTimeOutReq) (*FastTimeOutResp, error) {
	resp, err := m.doRpc(m.getPeer(info.Addr), request)
	if err != nil {
		return nil, err
	}
	return resp.(*FastTimeOutResp), nil
}

func (m *memRPC) LocalAddr() ServerAddr {
	return m.localAddr
}

func (m *memRPC) EncodeAddr(info *ServerInfo) []byte {
	return []byte(info.Addr)
}

func (m *memRPC) DecodeAddr(bytes []byte) ServerAddr {
	return ServerAddr(bytes)
}
