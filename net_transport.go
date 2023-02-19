package go_raft

import (
	. "github.com/fuyao-w/common-util"

	"bufio"
	"context"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type (
	netConn struct {
		remote ServerAddr
		c      net.Conn
		rw     *bufio.ReadWriter
	}
	ServerAddrProvider interface {
		GetAddr(id ServerID) (ServerAddr, error)
	}
	typConnPool  map[ServerAddr][]*netConn
	NetTransport struct {
		shutDown           shutDown
		timeout            time.Duration
		cmdCHan            chan *CMD
		netLayer           NetLayer
		connPoll           *connPool
		serverAddrProvider ServerAddrProvider
		processor          Processor
		heartbeatFastPath  fastPath
		TimeoutScale       int64
		ctx                *LockItem[ctx]
	}
	ctx struct {
		ctx    context.Context
		cancel context.CancelFunc
	}
	connPool struct {
		pool             *LockItem[typConnPool]
		maxSinglePoolNum int
	}
)

func (n *NetTransport) EncodeAddr(info *ServerInfo) []byte {
	return []byte(info.Addr)
}

func (n *NetTransport) DecodeAddr(bytes []byte) ServerAddr {
	return ServerAddr(bytes)
}

func (n *NetTransport) LocalAddr() ServerAddr {
	return ServerAddr(n.netLayer.Addr().String())
}

func (n *NetTransport) Consumer() <-chan *CMD {
	return n.cmdCHan
}

func (n *NetTransport) getServerAddr(info *ServerInfo) ServerAddr {
	if n.serverAddrProvider == nil {
		return info.Addr
	}
	addr, err := n.serverAddrProvider.GetAddr(info.ID)
	if err != nil {
		return info.Addr
	}
	return addr
}

func (n *NetTransport) sendRpc(conn *netConn, cmdType cmdType, request interface{}) error {
	data, err := defaultCmdConverter.Serialization(request)
	if err != nil {
		return err
	}
	if err = defaultPackageParser.Encode(conn.rw.Writer, cmdType, data); err != nil {
		return err
	}
	conn.rw.Flush()
	return nil
}
func (n *NetTransport) recvRpc(conn *netConn, resp interface{}) error {
	_, data, err := defaultPackageParser.Decode(conn.rw.Reader)
	if err != nil {
		return err
	}
	err = defaultCmdConverter.Deserialization(data, resp)
	if err != nil {
		return err
	}
	return nil
}
func (n *NetTransport) genericRPC(info *ServerInfo, cmdType cmdType, request, response interface{}) (err error) {
	conn, err := n.getConn(info)
	if err != nil {
		return err
	}
	if n.timeout > 0 {
		conn.c.SetDeadline(time.Now().Add(n.timeout))
	}
	defer func() {
		if err != nil {
			conn.Close()
		} else {
			n.connPoll.PutConn(conn)
		}
	}()
	if err = n.sendRpc(conn, cmdType, request); err != nil {
		return
	}

	return n.recvRpc(conn, response)
}
func (n *NetTransport) getConn(info *ServerInfo) (*netConn, error) {
	addr := n.getServerAddr(info)
	if conn := n.connPoll.GetConn(addr); conn != nil {
		return conn, nil
	}
	conn, err := n.netLayer.Dial(info.Addr, n.timeout)
	if err != nil {
		return nil, err
	}
	return newNetConn(info.Addr, conn), nil
}
func newNetConn(addr ServerAddr, conn net.Conn) *netConn {
	return &netConn{
		remote: addr,
		c:      conn,
		rw:     bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
	}
}
func (n *NetTransport) VoteRequest(info *ServerInfo, request *VoteRequest) (*VoteResponse, error) {
	var resp = new(VoteResponse)
	return resp, n.genericRPC(info, CmdVoteRequest, request, resp)
}

func (n *NetTransport) AppendEntries(info *ServerInfo, request *AppendEntryRequest) (*AppendEntryResponse, error) {
	var resp = new(AppendEntryResponse)
	return resp, n.genericRPC(info, CmdAppendEntry, request, resp)
}

func (n *NetTransport) AppendEntryPipeline(info *ServerInfo) (AppendEntryPipeline, error) {
	conn, err := n.getConn(info)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			conn.Close()
		} else {
			n.connPoll.PutConn(conn)
		}
	}()
	return newNetPipeline(n, conn), err
}

func (n *NetTransport) InstallSnapShot(info *ServerInfo, request *InstallSnapshotRequest, r io.Reader) (*InstallSnapshotResponse, error) {
	conn, err := n.getConn(info)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			conn.Close()
		} else {
			n.connPoll.PutConn(conn)
		}
	}()
	if n.timeout > 0 {
		conn.c.SetDeadline(time.Now().Add(func() time.Duration {
			timeout := n.timeout * time.Duration(request.Size/n.TimeoutScale)
			if timeout < n.timeout {
				timeout = n.timeout
			}
			return timeout
		}()))
	}
	if err = n.sendRpc(conn, CmdInstallSnapshot, request); err != nil {
		return nil, err
	}
	if _, err = io.Copy(conn.rw, r); err != nil {
		return nil, err
	}
	if err = conn.rw.Flush(); err != nil {
		return nil, err
	}

	var resp = new(InstallSnapshotResponse)
	if err = n.recvRpc(conn, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (n *NetTransport) SetHeartbeatFastPath(cb fastPath) {
	n.heartbeatFastPath = cb
}

func (n *NetTransport) FastTimeOut(info *ServerInfo, req *FastTimeOutReq) (*FastTimeOutResp, error) {
	var resp = new(FastTimeOutResp)
	return resp, n.genericRPC(info, CmdFastTimeout, req, resp)
}
func newConnPool(maxSinglePoolNum int) *connPool {
	p := &connPool{
		pool:             NewLockItem[typConnPool](map[ServerAddr][]*netConn{}),
		maxSinglePoolNum: maxSinglePoolNum,
	}
	return p
}
func (c *connPool) GetConn(addr ServerAddr) (conn *netConn) {
	c.pool.Action(func(t *typConnPool) {
		if list, ok := (*t)[addr]; ok {
			if len(list) == 0 {
				return
			}
			list = list[:len(list)-1]
			(*t)[addr] = list
		}
	})
	return
}

func (c *connPool) PutConn(conn *netConn) {
	c.pool.Action(func(t *typConnPool) {
		if c.maxSinglePoolNum <= len((*t)[conn.remote]) {
			conn.Close()
			return
		}
		(*t)[conn.remote] = append((*t)[conn.remote], conn)
	})
}

func (n *netConn) Close() {
	n.c.Close()
}

func NewNetTransport(conf *Conf) *NetTransport {
	logConnCtx, cancel := context.WithCancel(context.Background())
	t := &NetTransport{
		timeout:            conf.TransportTimeout,
		cmdCHan:            make(chan *CMD),
		netLayer:           conf.NetLayer,
		connPoll:           newConnPool(conf.MaxPool),
		serverAddrProvider: conf.ServerAddrProvider,
		processor:          newProcessorProxy(),
		TimeoutScale:       DefaultTimeoutScale,
		shutDown:           newShutDown(),
		ctx: NewLockItem(ctx{
			ctx:    logConnCtx,
			cancel: cancel,
		}),
	}

	return t
}

func genCtx() ctx {
	c, cancel := context.WithCancel(context.Background())
	return ctx{
		ctx:    c,
		cancel: cancel,
	}
}

func (n *NetTransport) CloseConnections() {
	n.connPoll.pool.Action(func(t *typConnPool) {
		for _, conns := range *t {
			for _, conn := range conns {
				conn.Close()
			}
		}
	})
	n.ctx.Action(func(t *ctx) {
		t.cancel()
		*t = genCtx()
	})
}
func (s *NetTransport) Stop() {
	s.shutDown.done(func() {
		s.netLayer.Close()
	})
}
func (n *NetTransport) Start() {
	var c int64
	for {
		conn, err := n.netLayer.Accept()
		if err != nil {
			if n.processError(err, c) {
				return
			}
		}
		c = 0
		go n.handleConn(n.ctx.Get().ctx, newNetConn("", conn))
	}
}

const baseDelay = 5 * time.Millisecond
const maxDelay = 1 * time.Second

func (s *NetTransport) processError(err error, count int64) (needEnd bool) {
	select {
	case <-s.shutDown.C:
		log.Printf("server shut down")
		return true
	default:
	}
	e, ok := err.(net.Error)
	if !ok {
		return true
	}
	switch {
	case e.Timeout():
		log.Printf("listener|Accept|err %s ", err)
		time.Sleep(func() (delay time.Duration) {
			delay = time.Duration(count) * baseDelay
			if delay > maxDelay {
				delay = maxDelay
			}
			return delay
		}() * time.Millisecond)
		return false
	}
	return true
}

func (n *NetTransport) handleConn(ctx context.Context, conn *netConn) error {
	defer conn.Close()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			cmdType, data, err := defaultPackageParser.Decode(conn.rw.Reader)
			if err != nil {
				log.Printf("processConnection|read from connection err : %s\n", err)
				return err
			}

			respData, err := n.processor.Do(cmdType, data)
			if err != nil {
				return err
			}
			err = defaultPackageParser.Encode(conn.rw.Writer, cmdType, respData.([]byte))
			conn.rw.Flush()
		}
	}
}

type netPipeline struct {
	conn         *netConn
	trans        *NetTransport
	doneCh       chan AppendEntriesFuture
	inProgressCh chan *appendEntriesFuture
	shutdown     bool
	shutdownCh   chan struct{}
	lock         sync.Mutex
}

func (n *netPipeline) AppendEntries(request *AppendEntryRequest) (AppendEntriesFuture, error) {
	af := newAppendEntriesFuture(request)
	if err := n.trans.sendRpc(n.conn, CmdAppendEntry, af.req); err != nil {
		return nil, err
	}
	select {
	case <-n.shutdownCh:
		return nil, ErrShutDown
	case n.inProgressCh <- af:
	}

	return af, nil
}

func (n *netPipeline) Consumer() <-chan AppendEntriesFuture {
	return n.doneCh
}

func (n *netPipeline) Close() error {
	n.conn.Close()
	return nil
}

func newNetPipeline(trans *NetTransport, conn *netConn) *netPipeline {
	pipeline := &netPipeline{
		conn:         conn,
		trans:        trans,
		doneCh:       make(chan AppendEntriesFuture, 128),
		inProgressCh: make(chan *appendEntriesFuture, 128),
		shutdownCh:   make(chan struct{}),
	}
	go pipeline.decodeResponses()
	return pipeline
}

func (n *netPipeline) decodeResponses() {
	for {
		select {
		case <-n.shutdownCh:
			return
		case af := <-n.inProgressCh:
			if err := n.trans.recvRpc(n.conn, af.response); err != nil {
				af.fail(err)
			} else {
				af.responded(af.response, nil)
			}
			select {
			case <-n.shutdownCh:
				return
			case n.doneCh <- af:
			}
		}
	}
}
