package go_raft

import (
	"bufio"
	"encoding/json"
	"net"
	"time"
)

const (
	CmdVoteRequest cmdType = iota + 1
	CmdAppendEntry
	CmdAppendEntryPipeline
	CmdInstallSnapshot
	CmdFastTimeout
)

// DefaultTimeoutScale is the default TimeoutScale in a NetworkTransport.
const DefaultTimeoutScale = 256 * 1024 // 256KB

type NetLayer interface {
	net.Listener
	// Dial is used to create a new outgoing connection
	Dial(peer ServerAddr, timeout time.Duration) (net.Conn, error)
}

type (
	fastPath func(cb *CMD) bool

	PackageParser interface {
		Encode(writer *bufio.Writer, cmdType cmdType, data []byte) (err error)
		Decode(reader *bufio.Reader) (cmdType, []byte, error)
	}

	CmdConvert interface {
		Deserialization(data []byte, i interface{}) error
		Serialization(i interface{}) (bytes []byte, err error)
	}

	// JsonCmdHandler 提供 json 的序列化能力
	JsonCmdHandler struct{}
)

var defaultCmdConverter = new(JsonCmdHandler)

func (j *JsonCmdHandler) Deserialization(data []byte, i interface{}) error {
	return json.Unmarshal(data, i)
}

func (j JsonCmdHandler) Serialization(i interface{}) (bytes []byte, err error) {
	return json.Marshal(i)
}
