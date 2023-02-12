package go_raft

import (
	"fmt"
	"testing"
	"time"
)

type testServerProcessor struct {
}

func (t *testServerProcessor) Do(u uint8, i interface{}) (interface{}, error) {
	fmt.Println("type", u, "i", i.(*VoteRequest).RPCHeader)
	return &VoteResponse{
		RPCHeader: &RPCHeader{
			ID:   "tes",
			Addr: "add",
		},
		VoteGranted: true,
	}, nil
}

func TestServer(t *testing.T) {

	transport := NewNetTransport(
		Config,
		new(DefaultPackageParser),
		new(JsonCmdHandler),
		new(testServerProcessor),
	)
	go func() {
		for i := uint64(0); ; i++ {
			time.Sleep(time.Second)

			resp, err := transport.VoteRequest(&ServerInfo{
				ID:   "!2312",
				Addr: ServerAddr(Config.LocalAddr.String()),
			}, &VoteRequest{
				RPCHeader: &RPCHeader{
					ID:   "testID",
					Addr: "testAddr",
				},
				Term:         i,
				LastLogIndex: 100,
				LastLogTerm:  123123,
			})
			fmt.Println(err, resp.RPCHeader)

		}
	}()
	transport.Start()

}
