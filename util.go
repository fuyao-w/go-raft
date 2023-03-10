package go_raft

import (
	. "github.com/fuyao-w/common-util"
	"io"
	"sync/atomic"

	crand "crypto/rand"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"os"
	"os/signal"

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
	state   *LockItem[bool]
	C       chan struct{}
}

func newShutDown() shutDown {
	return shutDown{
		state: NewLockItem[bool](),
		C:     make(chan struct{}),
	}
}

func (s *shutDown) done(act func(oldState bool)) {
	s.state.Action(func(t *bool) {
		old := *t
		*t = true

		close(s.C)
		if act != nil {
			act(old)
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

type AtomicVal[T any] struct {
	v atomic.Value
}

func NewAtomicVal[T any]() *AtomicVal[T] {
	return &AtomicVal[T]{}
}
func (a AtomicVal[T]) Load() T {
	return a.v.Load().(T)
}
func (a AtomicVal[T]) Store(t T) {
	a.v.Store(t)
}

type Logger interface {
	Infof(format string, v ...any)
	Info(v ...any)
	Errorf(format string, v ...any)
	Error(v ...any)
	Warnf(format string, v ...any)
	Warn(v ...any)
	Debugf(format string, v ...any)
	Debug(v ...any)
}

func newCounterReader(r io.Reader) *countingReader {
	return &countingReader{reader: r}
}

// countingReader 支持随时查询读取长度
type countingReader struct {
	reader io.Reader
	count  int64
}

func (r *countingReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	atomic.AddInt64(&r.count, int64(n))
	return
}

func (r *countingReader) Count() int64 {
	return atomic.LoadInt64(&r.count)
}
