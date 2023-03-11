package go_raft

import (
	"bytes"
	"github.com/gookit/goutil/dump"
	"io"
	"testing"
	"time"
)

func TestMemTransport(t *testing.T) {
	handler := func(rpc RpcInterface) {
		for i := 0; ; i++ {
			select {
			case cmd := <-rpc.Consumer():
				switch cmd.CmdType {
				case CmdInstallSnapshot:
					sink := &memSnapshotSink{
						buf: &bytes.Buffer{},
						meta: &SnapShotMeta{
							Version:            0,
							ID:                 "",
							Index:              0,
							Term:               0,
							configuration:      configuration{},
							configurationIndex: 0,
							Size:               0,
						},
					}
					//var buf = make([]byte, 3)
					//cmd.Reader.Read(buf)
					//t.Log(string(buf))
					io.Copy(sink, cmd.Reader)
					t.Log(sink.buf.String())

				case CmdAppendEntry:
					cmd.Response <- &AppendEntryResponse{
						Term:         uint64(i + 1),
						LastLogIndex: 111,
						Success:      true,
					}

				}
			}
		}
	}
	a := newMemRpc()
	b := newMemRpc()
	go handler(a)
	go handler(b)
	a.Connect(b.localAddr, b)
	b.Connect(a.localAddr, a)
	bInfo := &ServerInfo{Addr: b.localAddr}
	//a.VoteRequest(bInfo, &VoteRequest{
	//	RPCHeader: &RPCHeader{
	//		ID:     "",
	//		Addr:   b.localInfo,
	//		ErrMsg: "",
	//	},
	//	term:               1,
	//	LastLogIndex:       1,
	//	LastLogTerm:        1,
	//	LeadershipTransfer: false,
	//})
	appendEntryReq := &AppendEntryRequest{
		RPCHeader:    nil,
		Term:         0,
		LeaderID:     "",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []*LogEntry{
			{
				Term:      1,
				Data:      []byte("12"),
				Index:     3,
				Type:      4,
				CreatedAt: time.Now(),
			},
		},
		LeaderCommit: 0,
	}

	//a.AppendEntries(bInfo, appendEntryReq)
	pipeline, _ := a.AppendEntryPipeline(bInfo)
	go func() {
		for {
			select {
			case af := <-pipeline.Consumer():
				resp, _ := af.Response()
				t.Log(dump.Format(resp))
			}

		}
	}()
	for i := 0; i < 100; i++ {
		_, _ = pipeline.AppendEntries(appendEntryReq)
	}

	a.InstallSnapShot(bInfo, &InstallSnapshotRequest{
		Size: 100,
	}, bytes.NewBuffer([]byte("123")))
	time.Sleep(time.Second)

	a.DisconnectAll()

}
