package go_raft

import (
	. "github.com/fuyao-w/common-util"
	"github.com/gookit/goutil/dump"
	"testing"
)

var testLog = newMemoryStore()

func buildLog(data ...Tuple[uint64, string]) (logs []*LogEntry) {
	for _, datum := range data {
		logs = append(logs, &LogEntry{
			Data:  []byte(datum.B),
			Index: datum.A,
		})
	}
	return
}
func TestMemLog(t *testing.T) {
	getCheck := func(idx uint64, target string) {
		log, err := testLog.GetLog(idx)
		if err != nil {
			t.Log("getcheck err ", err)
			return
		}
		m := string(log.Data)
		if m != target {
			t.Fatalf("m:%s ,target :%s ", m, target)
		}
	}

	testLog.SetLogs(buildLog(
		BuildTuple(uint64(1), "1"),
		BuildTuple(uint64(2), "2"),
		BuildTuple(uint64(3), "3"),
	))
	getCheck(1, "1")
	getCheck(2, "2")
	getCheck(3, "3")
	t.Log(testLog.LastIndex())
	t.Log(testLog.FirstIndex())
	testLog.DeleteRange(2, 3)
	dump.Println(testLog)
}

func TestKV(t *testing.T) {
	testLog.SetUint64("1", 1)
	t.Log(testLog.GetUint64("1"))
	testLog.Set("1", "x")
	t.Log(testLog.Get("1"))
}
