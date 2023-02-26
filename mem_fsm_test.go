package go_raft

import (
	. "github.com/fuyao-w/common-util"
	"github.com/gookit/goutil/dump"
	"testing"
)

func TestFsm(t *testing.T) {
	fsm := menFSM{}

	testLog.SetLogs(buildLog(
		BuildTuple(uint64(1), "1"),
		BuildTuple(uint64(2), "2"),
		BuildTuple(uint64(3), "3")))

	dump.Println(fsm)

}
