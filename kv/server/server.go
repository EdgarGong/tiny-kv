package server

import (
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	sr, _ := server.storage.Reader(req.Context)
	defer sr.Close()
	val, err := sr.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	} else if val == nil {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
			Value:    nil,
		}, nil
	} else {
		return &kvrpcpb.RawGetResponse{
			Value: val,
		}, nil
	}
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	err := server.storage.Write(req.Context, []storage.Modify{{
		Data: storage.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		}},
	})
	if err != nil {
		return nil, err
	} else {
		return &kvrpcpb.RawPutResponse{}, nil
	}
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	err := server.storage.Write(req.Context, []storage.Modify{{
		Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		}},
	})
	if err != nil {
		return nil, err
	} else {
		return &kvrpcpb.RawDeleteResponse{}, nil
	}
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	sr, _ := server.storage.Reader(req.Context)
	defer sr.Close()
	iter := sr.IterCF(req.Cf)
	defer iter.Close()
	res := &kvrpcpb.RawScanResponse{}
	iter.Seek(req.StartKey)
	for i := uint32(0); i < req.Limit && iter.Valid(); i++ {
		key := iter.Item().Key()
		value, err := iter.Item().Value()
		if err != nil {
			return nil, err
		}
		res.Kvs = append(res.Kvs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
		iter.Next()
	}

	return res, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.

//Any request might cause a region error,
//these should be handled in the same way as for the raw requests.
func getErr(err error, resp *kvrpcpb.GetResponse) (*kvrpcpb.GetResponse, error) {
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionErr.RequestErr
		return resp, nil
	}
	return nil, err
}

func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}
	sr, err := server.storage.Reader(req.Context)
	defer sr.Close()
	if err != nil {
		return getErr(err, resp)
	}
	txn := mvcc.NewMvccTxn(sr, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return getErr(err, resp)
	}
	if lock != nil && req.Version >= lock.Ts {
		//????? why should req.Version >= lock.Ts?

		//If the key to be read is locked by another transaction
		//at the time of the KvGet request,
		//then TinyKV should return an error.

		// but if req.Version < lock.Ts, the req is out of date
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.Key,
				LockTtl:     lock.Ttl,
			},
		}
		return resp, nil
	}

	value, err := txn.GetValue(req.Key)
	if err != nil {
		return getErr(err, resp)
	}
	if value == nil {
		resp.Value = nil
		resp.NotFound = true
	}
	resp.Value = value

	return resp, nil
}

//Any request might cause a region error,
//these should be handled in the same way as for the raw requests.
func preWriteErr(err error, resp *kvrpcpb.PrewriteResponse) (*kvrpcpb.PrewriteResponse, error) {
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionErr.RequestErr
		return resp, nil
	}
	return nil, err
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	if len(req.Mutations) == 0 {
		return &kvrpcpb.PrewriteResponse{}, nil
	}
	resp := &kvrpcpb.PrewriteResponse{}
	sr, err := server.storage.Reader(req.Context)
	defer sr.Close()
	if err != nil {
		return preWriteErr(err, resp)
	}
	var keyErrs []*kvrpcpb.KeyError
	txn := mvcc.NewMvccTxn(sr, req.StartVersion)
	for _, mutation := range req.Mutations {
		// solve all the mutations(writes)
		write, ts, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			return preWriteErr(err, resp)
		}
		if write != nil && ts >= req.StartVersion {
			// req.StartVersion is out of date
			// so there's a write confilct
			keyErrs = append(keyErrs, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: ts,
					Key:        mutation.Key,
					Primary:    req.PrimaryLock,
				},
			})
			//???
			continue
		}
		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			return preWriteErr(err, resp)
		}
		if lock != nil && req.StartVersion != lock.Ts {
			// ??? why is not >= but != ?
			keyErrs = append(keyErrs, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         mutation.Key,
					LockTtl:     lock.Ttl,
				},
			})
			continue
		}
		if mutation.Op == kvrpcpb.Op_Put {
			txn.PutLock(mutation.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      req.StartVersion,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindPut,
			})
			txn.PutValue(mutation.Key, mutation.Value)
		} else if mutation.Op == kvrpcpb.Op_Del {
			txn.PutLock(mutation.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      req.StartVersion,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindDelete,
			})
			txn.DeleteValue(mutation.Key)
		} else {
			err := fmt.Errorf("op error")
			return nil, err
		}
	}
	if len(keyErrs) > 0 {
		resp.Errors = keyErrs
		return resp, nil
	}
	if err = server.storage.Write(req.Context, txn.Writes()); err != nil {
		return preWriteErr(err, resp)
	}

	return resp, nil
}

//Any request might cause a region error,
//these should be handled in the same way as for the raw requests.
func commitErr(err error, resp *kvrpcpb.CommitResponse) (*kvrpcpb.CommitResponse, error) {
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionErr.RequestErr
		return resp, nil
	}
	return nil, err
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	if len(req.Keys) == 0 {
		return &kvrpcpb.CommitResponse{}, nil
	}
	resp := &kvrpcpb.CommitResponse{}
	sr, err := server.storage.Reader(req.Context)
	defer sr.Close()
	if err != nil {
		return commitErr(err, resp)
	}

	txn := mvcc.NewMvccTxn(sr, req.StartVersion)

	//?????
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return commitErr(err, resp)
		}
		if lock == nil {
			//KvCommit will fail if the key is not locked
			return resp, nil
		}
		if req.StartVersion != lock.Ts {
			//KvCommit will fail if the key is locked by another transaction.
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "the key is locked by another transaction",
			}
			return resp, nil
		}
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}

	if err = server.storage.Write(req.Context, txn.Writes()); err != nil {
		return commitErr(err, resp)
	}
	return resp, nil
}

//Any request might cause a region error,
//these should be handled in the same way as for the raw requests.
func scanErr(err error, resp *kvrpcpb.ScanResponse) (*kvrpcpb.ScanResponse, error) {
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionErr.RequestErr
		return resp, nil
	}
	return nil, err
}

// KvScan scan KV pairs from StartKey to StartKey+limit
func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return scanErr(err, resp)
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()
	var pairs []*kvrpcpb.KvPair
	for i := 0; i < int(req.Limit); i++ {
		key, value, err := scanner.Next()
		if err != nil {
			return scanErr(err, resp)
		}
		if key == nil {
			break
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return scanErr(err, resp)
		}
		if lock != nil && lock.Ts <= req.Version {
			pairs = append(pairs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						LockTtl:     lock.Ttl,
						Key:         key,
					},
				},
			})
		} else if value != nil {
			pairs = append(pairs, &kvrpcpb.KvPair{
				Key:   key,
				Value: value,
			})
		}
	}
	resp.Pairs = pairs
	return resp, nil
}

//KvCheckTxnStatus, KvBatchRollback, and KvResolveLock are used by a client when it encounters some kind of conflict when trying to write a transaction.
//Each one involves changing the state of existing locks.

//Any request might cause a region error,
//these should be handled in the same way as for the raw requests.
func checkTxnErr(err error, resp *kvrpcpb.CheckTxnStatusResponse) (*kvrpcpb.CheckTxnStatusResponse, error) {
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionErr.RequestErr
		return resp, nil
	}
	return nil, err
}

//KvCheckTxnStatus checks for timeouts, removes expired locks and returns the status of the lock.
func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	pKey := req.PrimaryKey
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return checkTxnErr(err, resp)
	}
	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	write, ts, err := txn.CurrentWrite(pKey)
	if err != nil {
		return checkTxnErr(err, resp)
	}
	if write != nil {
		if write.Kind != mvcc.WriteKindRollback {
			resp.CommitVersion = ts
		}
		return resp, nil
	}
	lock, err := txn.GetLock(pKey)
	if err != nil {
		return checkTxnErr(err, resp)
	}
	if lock == nil {
		//Lock Not Exist
		resp.Action = kvrpcpb.Action_LockNotExistRollback

		write := &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		}
		txn.PutWrite(pKey, req.LockTs, write)
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return checkTxnErr(err, resp)
		}

		return resp, nil
	}
	if mvcc.PhysicalTime(req.CurrentTs) >= mvcc.PhysicalTime(lock.Ts)+lock.Ttl {
		//handle timeouts
		//when calculating timeouts, we must only use the physical part of the timestamp.
		//To do this you may find the PhysicalTime function in transaction.go useful.

		resp.Action = kvrpcpb.Action_TTLExpireRollback

		//removes expired locks
		txn.DeleteLock(pKey)
		txn.DeleteValue(pKey)

		write := &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		}
		txn.PutWrite(pKey, req.LockTs, write)
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return checkTxnErr(err, resp)
		}
	}
	return resp, nil
}

//Any request might cause a region error,
//these should be handled in the same way as for the raw requests.
func batchErr(err error, resp *kvrpcpb.BatchRollbackResponse) (*kvrpcpb.BatchRollbackResponse, error) {
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionErr.RequestErr
		return resp, nil
	}
	return nil, err
}

//KvBatchRollback checks that a key is locked by the current transaction, and if so removes the lock,
//deletes any value and leaves a rollback indicator as a write.
func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	if len(req.Keys) == 0 {
		return &kvrpcpb.BatchRollbackResponse{}, nil
	}
	resp := &kvrpcpb.BatchRollbackResponse{}
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return batchErr(err, resp)
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.WaitForLatches(req.Keys) //?????
	defer server.Latches.ReleaseLatches(req.Keys)
	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return batchErr(err, resp)
		}
		if write != nil {
			if write.Kind != mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{Abort: "error occur"}
				return resp, nil
			} else {
				continue
			}
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			return batchErr(err, resp)
		}
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
		if lock != nil && req.StartVersion == lock.Ts {
			//if the key is locked,
			//removes the lock, deletes any value and leaves a rollback indicator as a write.
			txn.DeleteLock(key)
			txn.DeleteValue(key)
		}
		//} else {
		//	txn.PutWrite(key, req.StartVersion, write)
		//}
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return batchErr(err, resp)
	}
	return resp, nil
}

//Any request might cause a region error,
//these should be handled in the same way as for the raw requests.
func resolvedErr(err error, resp *kvrpcpb.ResolveLockResponse) (*kvrpcpb.ResolveLockResponse, error) {
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionErr.RequestErr
		return resp, nil
	}
	return nil, err
}

//KvResolveLock inspects a batch of locked keys and either rolls them all back or commits them all.
func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	if req.StartVersion == 0 {
		return &kvrpcpb.ResolveLockResponse{}, nil
	}
	resp := &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return resolvedErr(err, resp)
	}

	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close()
	var keys [][]byte
	for ; iter.Valid(); iter.Next() {
		//inspects a batch of locked keys
		item := iter.Item()
		v, err := item.ValueCopy(nil)
		if err != nil {
			return resp, err
		}
		lock, err := mvcc.ParseLock(v)
		if err != nil {
			return resp, err
		}
		if req.StartVersion == lock.Ts {
			keys = append(keys, item.KeyCopy(nil))
		}
	}

	if len(keys) == 0 {
		return resp, nil
	}

	if req.CommitVersion != 0 {
		//commits them all
		commitResp, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
			StartVersion:  req.StartVersion,
		})
		resp.RegionError = commitResp.RegionError
		resp.Error = commitResp.Error
		return resp, err
	} else {
		//rolls them all back
		rollResp, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			Keys:         keys,
			StartVersion: req.StartVersion,
		})
		resp.RegionError = rollResp.RegionError
		resp.Error = rollResp.Error
		return resp, err
	}

}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
