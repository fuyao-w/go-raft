package go_raft

import (
	. "github.com/fuyao-w/common-util"
	"sync/atomic"
	"unsafe"

	"errors"
	"fmt"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

func TestNewSeed(t *testing.T) {
	start := time.Now()
	select {
	case <-randomTimeout(time.Second):
	}
	t.Log(time.Now().Sub(start))
}

func TestShutDown(t *testing.T) {
	s := shutDown{
		dataBus: DataBus{},
		C:       make(chan struct{}),
	}
	s.AddCallback(func(event int, param interface{}) {
		fmt.Println("shutdown")
	})
	s.AddCallback(func(event int, param interface{}) {
		fmt.Println("shutdown")
	})
	//s.WaitForShutDown()
}
func check(cond bool, t *testing.T, args ...any) {
	if !cond {
		t.Log(args...)
		t.FailNow()
	}
}
func TestLock(t *testing.T) {
	check := func(cond bool) {
		if !cond {
			t.Log("not equal")
			t.FailNow()
		}
	}
	l := LockItem[ServerInfo]{}
	t.Log(l.Get())
	target := ServerInfo{
		Suffrage: 1,
		ID:       "1",
		Addr:     "1",
	}
	l.Set(target)
	check(l.Get() == target)
	l.Action(func(s *ServerInfo) {
		s.ID = "2"
	})
	target.ID = "2"
	check(l.Get() == target)
}

func TestSort(t *testing.T) {
	s := []uint64{5, 1, 10, 2, 7, 11, 3}
	SortSlice(s)
	for i, u := range s {
		if i == 0 {
			continue
		}
		if u < s[i-1] {
			t.Fatal("顺序不是从小大")
			t.FailNow()
		}
	}
}

func TestUUID(t *testing.T) {
	var (
		result = map[string]struct{}{}
		count  = 50
	)
	for i := 0; i < count; i++ {
		result[generateUUID()] = struct{}{}

	}
	if len(result) != count {
		t.Log(result)
		t.Fatal("uuid repeat")
		t.FailNow()
	}
}

func TestAsyncNotify(t *testing.T) {

	check(!asyncNotify(make(chan struct{})), t, "1 should send fail")
	check(asyncNotify(make(chan struct{}, 1)), t, "2 should send succ")
	notify := make(chan struct{})
	go func() {
		<-notify
	}()
	time.Sleep(10 * time.Millisecond)
	check(asyncNotify(notify), t, "3 should send succ")
}

func TestOverrideNotifyBool(t *testing.T) {
	ch := make(chan bool, 1)
	for i := 0; i < 6; i++ {
		overrideNotifyBool(ch, i%2 == 1)
	}
	check(<-ch, t, "should true")

	func() {
		var wg errgroup.Group

		for i := 0; i < 1000; i++ {
			wg.Go(func() (e error) {
				defer func() {
					if recover() != nil {
						e = errors.New("race")
					}
				}()
				overrideNotifyBool(ch, true)
				return
			})
		}
		if wg.Wait() == nil {
			t.Log("not race")
			t.FailNow()
		}
	}()

}

func TestAtomicBool(t *testing.T) {

	setPro(true)
	t.Log(inPro())
	setPro(false)
	t.Log(inPro())
}

var in *bool

func inPro() bool {
	if in == nil {
		return false
	}
	pointer := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&in)))
	return *(*bool)(pointer)
}
func setPro(inProgress bool) {

	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&in)), unsafe.Pointer(&inProgress))
}
