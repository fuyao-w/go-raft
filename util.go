package go_raft

import (
	crand "crypto/rand"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
)

func init() {
	rand.Seed(newSeed())
}

func newSeed() int64 {
	r, err := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		panic(fmt.Errorf("failed to read random bytes :%s", err))
	}
	return r.Int64()
}

// shutDown 处理关机逻辑，并提供回调信息
type shutDown struct {
	dataBus DataBus
	state   lockItem[bool]
	C       chan struct{}
}

func newShutDown() shutDown {
	return shutDown{
		state: lockItem[bool]{},
		C:     make(chan struct{}),
	}
}

func (s *shutDown) done(act func()) {
	s.state.Action(func(t *bool) {
		*t = true
		close(s.C)
		if act != nil {
			act()
		}
		s.dataBus.Publish(0, nil)
	})
}

// WaitForShutDown 阻塞直到关机
func (s *shutDown) WaitForShutDown() {
	notify := make(chan os.Signal, 1)
	// kill 默认会发送 syscall.SIGTERM 信号
	// kill -2 发送 syscall.SIGINT 信号，我们常用的Ctrl+C就是触发系统SIGINT信号
	// kill -9 发送 syscall.SIGKILL 信号，但是不能被捕获，所以不需要添加它
	// signal.Notify把收到的 syscall.SIGINT或syscall.SIGTERM 信号转发给quit
	signal.Notify(notify, syscall.SIGINT, syscall.SIGTERM) // 此处不会阻塞
	<-notify
	s.done(nil)
}

func (s *shutDown) AddCallback(obs observer) {
	s.dataBus.AddObserver(obs)
}

type (
	// observer 回调函数类型
	observer func(event int, param interface{})
	// DataBus 提供发布、订阅功能
	DataBus struct {
		observers []observer
	}
)

// AddObserver 添加订阅者
func (d *DataBus) AddObserver(obs observer) {
	d.observers = append(d.observers, obs)
}

// Publish 触发事件
func (d *DataBus) Publish(event int, param interface{}) {
	for _, obs := range d.observers {
		obs(event, param)
	}
}

// lockItem 通过锁保护 item 的 访问
type lockItem[T any] struct {
	item T
	lock sync.Mutex
}

func newLockItem[T any](t T) lockItem[T] {
	return lockItem[T]{
		item: t,
	}
}

func (s *lockItem[T]) Action(act func(t *T)) {
	s.lock.Lock()
	defer s.lock.Unlock()
	act(&s.item)
}
func (s *lockItem[T]) Get() T {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.item
}
func (s *lockItem[T]) Set(t T) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.item = t
}

func max[T uint64 | time.Duration](a, b T) T {
	if a > b {
		return a
	}
	return b
}

func min[T uint64 | time.Duration](a, b T) T {
	if a > b {
		return a
	}
	return b
}

// randomTimeout 返回 t 到 2 x t 时间的随机时间
func randomTimeout(t time.Duration) <-chan time.Time {
	if t == 0 {
		return nil
	}
	return time.After(t + time.Duration(rand.Int63())%t)
}

func generateUUID() string {
	var buf = make([]byte, 1<<4)
	if _, err := crand.Read(buf); err != nil {
		panic(fmt.Errorf("failed to read random bytes :%s", err))
	}
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		buf[:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:],
	)
}

// asyncNotify 不阻塞的给 chan 发送一个信号，并返回是否发送成功
func asyncNotify(ch chan struct{}) bool {
	select {
	case ch <- struct{}{}:
		return true
	default:
		return false
	}
}

// overrideNotifyBool 通知一个 bool 类型的 channel, 如果 channel 中已经有值了，则会覆盖
// channel 长度必须为 1 ，如果并发访问会 panic
func overrideNotifyBool(ch chan bool, v bool) {
	for i := 0; i < 2; i++ {
		select {
		case ch <- v:
			// 发送成功
			return
		case <-ch:
			// 上次投递的没人消费
		}
	}
	// 如果循环两次说明有其他线程在并发投递
	panic("race:channel was send concurrently")
}

// sortSlice 小 -> 大
func sortSlice[S ~uint64](s []S) {
	sort.Slice(s, func(i, j int) bool {
		return s[i] < s[j]
	})
}

func Ptr[T any](t T) *T {
	return &t
}
