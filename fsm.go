package go_raft

import (
	"io"
)

type (
	LogFSM interface {
		Apply(entry *LogEntry) interface{}
		SnapShot() (FSMSnapShot, error)
		ReStore(io.ReadCloser) error
	}
	BatchLogFSM interface {
		BatchApply([]*LogEntry) []interface{}
		LogFSM
	}
)

type FSMSnapShot interface {
	Persist(sink SnapShotSink) error
	Release()
}

func (r *Raft) runLogFSM() {
	var lastIndex, lastTerm uint64
	configurationStore, configStoreOK := r.fsm.(ConfigurationStore)
	applySingle := func(req *commitTuple) {
		var resp interface{}
		defer func() {
			if req.future != nil {
				req.future.responded(resp, nil)
			}
		}()
		switch req.log.Type {
		case LogCommand:
			resp = r.fsm.Apply(req.log)
		case LogConfiguration:
			if !configStoreOK {
				return
			}
			configurationStore.StoreConfiguration(req.log.Index, DecodeConfiguration(req.log.Data))
		}
		lastIndex = req.log.Index
		lastTerm = req.log.Term
	}
	restore := func(req *restoreFuture) {
		meta, readerCloser, err := r.snapShotStore.Open(req.ID)
		if err != nil {
			req.fail(err)
			return
		}
		defer func() { readerCloser.Close() }()
		if err = r.fsm.ReStore(readerCloser); err != nil {
			req.fail(err)
			return
		}

		// Update the last index and term
		lastIndex = meta.Index
		lastTerm = meta.Term
		req.success()
	}
	for {
		select {
		case <-r.shutDown.ch:
			return
		case inter := <-r.fsmMutateCh:
			switch req := inter.(type) {
			case []*commitTuple:
				for _, tuple := range req {
					applySingle(tuple)
				}
			case *restoreFuture:
				restore(req)
			}
		case req := <-r.fsmSnapshotCh:
			snapShot, err := r.fsm.SnapShot()
			if err != nil {
				req.fail(err)
			} else {
				req.responded(SnapShotFutureResp{
					term:        lastTerm,
					index:       lastIndex,
					fsmSnapShot: snapShot,
				}, nil)
			}
		}

	}

}

// fsmRestoreAndMeasure wraps the Restore call on an FSM to consistently measure
// and report timing metrics. The caller is still responsible for calling Close
// on the source in all cases.
func fsmRestoreAndMeasure(fsm LogFSM, source io.ReadCloser, snapshotSize int64) error {
	return fsm.ReStore(source)
}
