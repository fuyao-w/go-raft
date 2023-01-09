package go_raft

import (
	"fmt"
	"log"
	"net"
	"time"
)

type (
	NetConn interface {
		GetConnection(peer *ServerInfo, timeout time.Duration) (*netConn, error)
	}
	Client struct {
		NetConn
		CmdConvert
	}
	ClientConn   struct{}
	ClientOption struct {
		NetConn    NetConn
		CmdConvert CmdConvert
	}
)

func withNetConn(conn NetConn) func(opt *ClientOption) {
	return func(opt *ClientOption) {
		opt.NetConn = conn
	}
}
func withClientCmdConvert(cmdConvert CmdConvert) func(opt *ClientOption) {
	return func(opt *ClientOption) {
		opt.CmdConvert = cmdConvert
	}
}

func (c *ClientConn) GetConnection(peer *ServerInfo, timeout time.Duration) (*netConn, error) {
	conn, err := net.DialTimeout("tcp", peer.Addr, timeout)
	if err != nil {
		fmt.Printf("client|call err:%s", err)
		log.Printf("client|call err:%s", err)
		return nil, err
	}
	return newNetConn(conn), nil
}
func NewClient(options ...func(option *ClientOption)) *Client {
	var (
		netConn    NetConn    = new(ClientConn)
		cmdConvert CmdConvert = new(JsonCmdHandler)
	)

	client := &Client{
		NetConn: new(ClientConn),
	}
	var opt ClientOption
	for _, do := range options {
		do(&opt)
	}
	if opt.NetConn != nil {
		netConn = opt.NetConn
	}
	if opt.CmdConvert != nil {
		client.CmdConvert = opt.CmdConvert
	}

	return &Client{
		NetConn:    netConn,
		CmdConvert: cmdConvert,
	}

}

func (s *NetTransport) Dial(peer *ServerInfo, cmdType uint8, request interface{}, response interface{}) error {
	conn, err := s.client.GetConnection(peer, time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err = s.sendRpc(conn, cmdType, request); err != nil {
		return err
	}
	return s.decodeRpc(conn, response)
}

func (s *NetTransport) sendRpc(conn *netConn, cmdType uint8, request interface{}) (err error) {
	defer func() {
		if err != nil {
			conn.Conn.Close()
		}
	}()
	data, err := s.client.Serialization(request)
	if err != nil {
		return err
	}
	err = s.packageParser.Encode(conn.writer, cmdType, data)
	if err != nil {
		return err
	}
	conn.writer.Flush()
	return nil
}

func (s *NetTransport) decodeRpc(conn *netConn, response interface{}) (err error) {
	defer func() {
		if err != nil {
			conn.Conn.Close()
		}
	}()
	_, respData, err := s.packageParser.Decode(conn.reader)
	if err != nil {
		return err
	}

	return s.client.Deserialization(respData, response)
}
