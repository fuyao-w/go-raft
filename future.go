package go_raft

import (
	"errors"
	"io"
	"sync"
	"time"
)

var (
	FutureErrTimeout   = errors.New("time out")
	FutureErrNotLeader = errors.New("not leader")
)

type useSnapShotFuture = func() (SnapShotMeta, io.ReadCloser, error)

// nilRespFuture Future 默认不需要返回值的类型
type nilRespFuture = interface{}

// Future 用于异步提交，Response 会同步返回，可以重复调用
type Future[T any] interface {
	Response() (T, error)
}

// defaultFuture 默认不需要返回值的 Future
type defaultFuture = Future[nilRespFuture]

type defaultDeferResponse = deferResponse[nilRespFuture]

type deferResponse[T any] struct {
	err        error
	once       *sync.Once
	timeout    chan time.Time
	errCh      chan error
	response   T
	ShutdownCh chan struct{}
}

func (d *deferResponse[_]) init() {
	d.errCh = make(chan error, 1)
	d.once = new(sync.Once)
}

func (d *deferResponse[T]) Response() (T, error) {
	d.once.Do(func() {
		select {
		case d.err = <-d.errCh:
		case <-d.timeout:
			d.err = FutureErrTimeout
		}
	})
	return d.response, d.err
}

type LogFuture struct {
	deferResponse[any]
	log *LogEntry
}

func (l *LogFuture) Index() uint64 {
	return l.log.Index
}

// responded 返回响应结果，在调用该方法后 Response 就会返回，该方法不支持重复调用
func (d *deferResponse[T]) responded(resp T, err error) {
	d.response = resp
	d.errCh <- err
	close(d.errCh)
}

func (d *deferResponse[T]) success() {
	d.responded(nil, nil)
}
func (d *deferResponse[T]) fail(err error) {
	d.responded(nil, err)
}

type AppendEntriesFuture interface {
	Future[nilRespFuture]
	StartAt() time.Time
	Request() *AppendEntryRequest
}

type appendEntriesFuture struct {
	deferResponse[nilRespFuture]
	startAt time.Time
	req     *AppendEntryRequest
}

func newAppendEntriesFuture(req *AppendEntryRequest) *appendEntriesFuture {
	af := &appendEntriesFuture{
		startAt: time.Now(),
		req:     req,
	}
	af.init()
	return af
}
func (a *appendEntriesFuture) StartAt() time.Time {
	return a.startAt
}

func (a *appendEntriesFuture) Request() *AppendEntryRequest {
	return a.req
}

type configurationChangeFuture struct {
	LogFuture
	req configurationChangeRequest
}

type verifyFuture struct {
	deferResponse[nilRespFuture]
	notifyCh   chan *verifyFuture
	quorumSize int
	votes      int
	lock       sync.Mutex
}

func (v *verifyFuture) vote(leader bool) {
	v.lock.Lock()
	defer v.lock.Unlock()
	if v.notifyCh == nil {
		return
	}
	if leader {
		v.votes++
		if v.votes >= v.quorumSize {
			v.notify()
		}
	} else {
		v.notify()
	}
}

func (v *verifyFuture) notify() {
	v.notifyCh <- v
	v.notifyCh = nil
}

type userRestoreFuture struct {
	defaultDeferResponse
	meta   *SnapShotMeta
	reader io.Reader
}

type leadershipTransferFuture struct {
	defaultDeferResponse
	ServerInfo *ServerInfo
}

type configurationsGetFuture struct {
	deferResponse[configurations]
}
type configurationGetFuture struct {
	deferResponse[configuration]
}

// bootstrapFuture is used to attempt a live bootstrap of the cluster. See the
// Raft object's BootstrapCluster member function for more details.
type bootstrapFuture struct {
	defaultDeferResponse

	// configuration is the proposed bootstrap configuration to apply.
	configuration configuration
}

type (
	reqSnapShotFuture struct {
		deferResponse[SnapShotFutureResp]
	}
	SnapShotFutureResp struct {
		term, index uint64
		fsmSnapShot FSMSnapShot
	}
)

// userSnapshotFuture is used for waiting on a user-triggered snapshot to
// complete.
type userSnapshotFuture struct {
	deferResponse[useSnapShotFuture]
}

// restoreFuture is used for requesting an FSM to perform a
// snapshot restore. Used internally only.
type restoreFuture struct {
	defaultDeferResponse
	ID string
}

type shutDownFuture struct {
	raft *Raft
}

func (s *shutDownFuture) Response() (nilRespFuture, error) {
	if s.raft == nil {
		return nil, nil
	}
	s.raft.waitShutDown()

	if inter, ok := s.raft.rpc.(interface {
		Close() error
	}); ok {
		inter.Close()
	}
	return nil, nil
}

type ApplyFuture interface {
	IndexFuture
	Future[interface{}]
}
type IndexFuture interface {
	Index() uint64
	Future[nilRespFuture]
}

type errFuture[T any] struct {
	err error
}

func (e *errFuture[T]) Index() uint64 {
	return 0
}

func (e *errFuture[T]) Response() (t T, _ error) {
	return t, e.err
}
