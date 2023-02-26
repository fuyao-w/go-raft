package go_raft

import (
	. "github.com/fuyao-w/common-util"
	"io"
)

type (
	SnapShotStore interface {
		Open(id string) (*SnapShotMeta, io.ReadCloser, error)
		List() ([]*SnapShotMeta, error)
		Create(version SnapShotVersion, index, term uint64, configuration configuration, configurationIndex uint64, rpc RpcInterface) (SnapShotSink, error)
	}
	SnapShotSink interface {
		io.WriteCloser
		ID() string
		Cancel() error
	}
	SnapShotVersion uint64
	SnapShotMeta    struct {
		Version            SnapShotVersion
		ID                 string
		Index              uint64
		Term               uint64
		configuration      configuration
		configurationIndex uint64
		Size               int64
	}
)

const (
	SnapShotVersionMin SnapShotVersion = iota + 1
	SnapShotVersionMax
)

func (r *Raft) shouldBuildSnapShot() bool {
	_, index := r.getLastSnapShot()
	lastIndex, err := r.logStore.LastIndex()
	if err != nil {
		return false
	}
	return lastIndex-index > r.Config().SnapshotThreshold
}

func (r *Raft) compactLogEntries(index uint64) error {
	minLogIndex, err := r.logStore.FirstIndex()
	if err != nil {
		return err
	}
	trailingLogs := r.Config().TrailingLogs
	_, lastIndex := r.getLastLog()
	if lastIndex < trailingLogs {
		return nil
	}

	maxLogIndex := Min(index, lastIndex-trailingLogs)

	if minLogIndex > maxLogIndex {
		return nil
	}

	return r.logStore.DeleteRange(minLogIndex, maxLogIndex)
}
func (r *Raft) buildSnapShot() (string, error) {

	req := &reqSnapShotFuture{}
	req.init()
	select {
	case r.fsmSnapshotCh <- req:
	case <-r.shutDown.C:
		return "", ErrShutDown
	}

	sresp, err := req.Response()
	if err != nil {
		return "", err
	}

	defer sresp.fsmSnapShot.Release()

	configurationFuture := new(configurationsGetFuture)
	configurationFuture.init()
	select {
	case r.configurationsGetCh <- configurationFuture:
	case <-r.shutDown.C:
		return "", ErrShutDown
	}
	cresp, err := configurationFuture.Response()
	if err != nil {
		return "", nil
	}

	commit := cresp.commit
	commitIndex := cresp.commitIndex

	if sresp.index < commitIndex {
		return "", nil
	}
	sink, err := r.snapShotStore.Create(1, sresp.index, sresp.term, commit, commitIndex, r.rpc)
	if err != nil {
		return "", err
	}

	if err = sresp.fsmSnapShot.Persist(sink); err != nil {
		sink.Cancel()
		return "", err
	}
	if err = sink.Close(); err != nil {
		return "", err
	}
	r.setLastSnapShot(sresp.term, sresp.index)
	r.compactLogEntries(sresp.index)

	return sink.ID(), err
}
func (r *Raft) runSnapShot() {
	for {
		select {
		case <-randomTimeout(r.Config().SnapshotInterval):
			if !r.shouldBuildSnapShot() {
				continue
			}
			_, _ = r.buildSnapShot()

		case fu := <-r.userSnapShotFutureCh:
			if id, err := r.buildSnapShot(); err != nil {
				fu.fail(err)
			} else {
				fu.responded(func() (meta *SnapShotMeta, closer io.ReadCloser, err error) {
					return r.snapShotStore.Open(id)
				}, nil)
			}
		case <-r.shutDown.C:
			return
		}

	}
}
