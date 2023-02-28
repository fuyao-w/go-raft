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
	shutDown   shutDown
}

type menAppendEntryPipeline struct {
	sync.Mutex
	peer, trans  *memRPC
	doneCh       chan AppendEntriesFuture
	inProgressCh chan *memAppendEntriesInflight
	shutDown     shutDown
}
type memAppendEntriesInflight struct {
	af  *appendEntriesFuture
	cmd *CMD
}

func (pipe *menAppendEntryPipeline) decodeResponse() {
	timeout := pipe.trans.timeout
	for {
		select {
		case <-pipe.shutDown.C:
			return
		case inflight := <-pipe.inProgressCh:
			var timeoutCh <-chan time.Time
			if timeout > 0 {
				timeoutCh = time.After(timeout)
			}
			select {
			case rpcResp := <-inflight.cmd.Response:
				resp := rpcResp.(*AppendEntryResponse)
				inflight.af.responded(resp, nil)
				select {
				case pipe.doneCh <- inflight.af:
				case <-pipe.shutDown.C:
					return
				}
			case <-timeoutCh:
				inflight.af.responded(nil, ErrTimeout)
				select {
				case pipe.doneCh <- inflight.af:
				case <-pipe.shutDown.C:
					return
				}
			case <-pipe.shutDown.C:
				return
			}
		}
	}
}
func (pipe *menAppendEntryPipeline) AppendEntries(request *AppendEntryRequest) (AppendEntriesFuture, error) {
	pipe.Lock()
	defer pipe.Unlock()
	af := newAppendEntriesFuture(request)
	var timeout <-chan time.Time
	if t := pipe.trans.timeout; t > 0 {
		timeout = time.After(t)
	}

	cmd := CMD{
		CmdType:  CmdAppendEntryPipeline,
		Request:  request,
		Response: make(chan interface{}, 1),
	}
	select {
	case pipe.peer.consumerCh <- &cmd:
	case <-timeout:
		return nil, ErrTimeout
	case <-pipe.shutDown.C:
		return nil, ErrShutDown
	}
	select {
	case pipe.inProgressCh <- &memAppendEntriesInflight{af: af}:
	case <-pipe.shutDown.C:
		return nil, ErrPipelineShutdown
	}
	return af, nil
}

func (pipe *menAppendEntryPipeline) Consumer() <-chan AppendEntriesFuture {
	return pipe.doneCh
}

func (pipe *menAppendEntryPipeline) Close() error {
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
	pipe := &menAppendEntryPipeline{
		peer: m.getPeer(info.Addr),
	}
	go pipe.decodeResponse()
	return pipe, nil
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
