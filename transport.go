package go_raft

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

const (
	CmdVoteRequest uint8 = iota + 1
	CmdAppendEntry
	CmdAppendEntryPipeline
	CmdInstallSnapshot
)

type NetLayer interface {
	net.Listener
	// Dial is used to create a new outgoing connection
	Dial(peer *ServerInfo, timeout time.Duration) (netConn, error)
}

type (
	fastPath     func(cb *CMD) bool
	NetTransport struct {
		listener      net.Listener
		packageParser PackageParser
		processor     Processor
		client        *Client
		quit          chan struct{}
	}

	PackageParser interface {
		Encode(writer *bufio.Writer, cmdType uint8, data []byte) (err error)
		Decode(reader *bufio.Reader) (uint8, []byte, error)
	}
	netConn struct {
		net.Conn
		reader *bufio.Reader
		writer *bufio.Writer
	}
	CmdConvert interface {
		Deserialization(data []byte, i interface{}) error
		Serialization(i interface{}) (bytes []byte, err error)
	}

	// JsonCmdHandler 提供 json 的序列化能力
	JsonCmdHandler struct{}
)

func (j *JsonCmdHandler) Deserialization(data []byte, i interface{}) error {
	return json.Unmarshal(data, i)
}

func (j JsonCmdHandler) Serialization(i interface{}) (bytes []byte, err error) {
	return json.Marshal(i)
}

func newNetConn(conn net.Conn) *netConn {
	return &netConn{
		Conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}
}

func NewNetTransport(conf *Conf, packageParser PackageParser, cmdConvert CmdConvert, serverProcessor Processor) *NetTransport {
	listener, err := net.Listen(conf.LocalAddr.Network(), conf.LocalAddr.String())
	if err != nil {
		panic(fmt.Errorf("init server err : %s", err))
	}
	t := &NetTransport{
		// init server
		listener:      listener,
		packageParser: packageParser,
		processor:     newProcessorProxy(withCmdConvert(cmdConvert), withProcessor(serverProcessor)),
		// init client
		client: NewClient(withClientCmdConvert(cmdConvert)),
		quit:   make(chan struct{}),
	}

	return t
}
func (s *NetTransport) Stop() {
	close(s.quit)
	s.listener.Close()

}
func (s *NetTransport) Start() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.processError(err)
			return
		}
		go s.handleConn(newNetConn(conn))
	}
}

func (s *NetTransport) processError(err error) (needEnd bool) {
	select {
	case <-s.quit:
		log.Printf("server shut down")
		return true
	default:

	}
	if err, ok := err.(net.Error); ok {
		switch {
		case err.Timeout():
			log.Printf("listener|Accept|err %s ", err)
			time.Sleep(time.Second)
			return false
		}
	}
	log.Printf("accept err :%s", err)
	return false
}

func (s *NetTransport) handleConn(conn *netConn) error {
	defer conn.Close()

	cmdType, data, err := s.packageParser.Decode(conn.reader)
	if err != nil {
		log.Printf("processConnection|read from connection err : %s\n", err)
		return err
	}

	respData, err := s.processor.Do(cmdType, data)
	if err != nil {
		return err
	}
	err = s.packageParser.Encode(conn.writer, cmdType, respData.([]byte))
	conn.writer.Flush()
	return err
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

func (n *netPipeline) Consumer() chan AppendEntriesFuture {
	return n.doneCh
}

func (n *netPipeline) Close() error {
	return n.conn.Close()
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
			if err := n.trans.decodeRpc(n.conn, af.response); err != nil {
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
