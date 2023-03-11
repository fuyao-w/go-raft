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
			r.logger.Errorf("fsm restore fail to open snap shot, err :%s ", err)
			req.fail(err)
			return
		}
		defer func() { readerCloser.Close() }()
		if err = r.fsm.ReStore(readerCloser); err != nil {
			r.logger.Errorf("fsm restore fail to restore, err :%s ", err)
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
		case <-r.shutDownCh():
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
			snapshot, err := r.fsm.SnapShot()
			if err != nil {
				r.logger.Errorf("fsm  fail to get snap shot, err :%s ", err)
				req.fail(err)
			} else {
				req.responded(SnapShotFutureResp{
					term:        lastTerm,
					index:       lastIndex,
					fsmSnapShot: snapshot,
				}, nil)
			}
		}

	}

}

func fsmRestoreAndMeasure(fsm LogFSM, source io.ReadCloser, snapshotSize int64) error {

	return fsm.ReStore(source)
}
