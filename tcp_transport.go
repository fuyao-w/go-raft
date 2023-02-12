package go_raft

import (
	"net"
	"time"
)

type TcpLayer struct {
	listener  net.Listener
	advertise net.Addr
}

func newTcpLayer(l net.Listener, advertise net.Addr) NetLayer {
	return &TcpLayer{
		listener:  l,
		advertise: advertise,
	}
}
func (t *TcpLayer) Accept() (net.Conn, error) {
	return t.listener.Accept()
}

func (t *TcpLayer) Close() error {
	return t.listener.Close()
}

func (t *TcpLayer) Addr() net.Addr {
	if t.advertise != nil {
		return t.advertise
	}
	return t.listener.Addr()
}

func (t *TcpLayer) Dial(peer ServerAddr, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(peer), timeout)
}
