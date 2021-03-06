// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// applied entry means it has been processed (in handleRaftReady)
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstable logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	//headEntry pb.Entry
	FirstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hardState, _, _ := storage.InitialState()
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	entries, err := storage.Entries(firstIndex, lastIndex + 1)
	if err != nil {
		panic(err)
	}
	return &RaftLog{
		storage:         storage,
		committed:       hardState.GetCommit(),
		applied:         firstIndex - 1,
		stabled:         lastIndex,
		entries:         entries,
		pendingSnapshot: nil,
		//headEntry:       storage.HeadEntry(),
		FirstIndex: 	 firstIndex,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	// update the firstIndex
	stoFirst, _ := l.storage.FirstIndex()
	if stoFirst > l.FirstIndex{
		l.FirstIndex = stoFirst
		if len(l.entries) > 0{
			l.entries = l.entries[l.indexArray(stoFirst):]
		}
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) <= 0{
		return nil
	}
	return l.entries[l.stabled-l.FirstIndex+1:]
	//return l.entries[l.stabled:]
	/*
	if l.stabled >= uint64(len(l.entries)){
		return make([]pb.Entry, 0)
	}else {
		return l.entries[l.stabled - l.FirstIndex + 1:]
	}*/
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0{
		 //return l.entries[l.indexArray(l.applied)+1 : l.indexArray(l.committed)+1]
		//if l.applied > l.committed{
			//panic(ErrUnavailable)
		//}
		return l.entries[l.applied-l.FirstIndex+1 : l.committed-l.FirstIndex+1]
		//return l.entries[l.applied : l.committed]
	}
	return nil
}


// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	//return l.headEntry.Index + uint64(len(l.entries))
	//return uint64(len(l.entries))
	var snapIndex uint64
	if !IsEmptySnap(l.pendingSnapshot){
		snapIndex = l.pendingSnapshot.Metadata.Index
	}
	if len(l.entries) > 0 {
		return max(l.entries[len(l.entries)-1].Index, snapIndex)
	}
	i, _ := l.storage.LastIndex()
	return max(i,snapIndex)
}

// LastTerm :the last entry's term
func (l *RaftLog) LastTerm() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 { // to avoid overflow
		//return l.headEntry.Term
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	/*if i < l.headEntry.Index {
		return 0, ErrCompacted
	} else if i == l.headEntry.Index {
		return l.headEntry.Term, nil
	}*/
	/*if i < 0 {
		return 0, ErrCompacted
	} else if i == 0 {
		return 0, nil
	}*/
	if len(l.entries) > 0 && i >= l.FirstIndex {
		return l.entries[i-l.FirstIndex].Term, nil
	}else{
		term, err := l.storage.Term(i)
		if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot){
			//?
			if i == l.pendingSnapshot.Metadata.Index {//?
				return l.pendingSnapshot.Metadata.Term, nil
			}
			if i < l.pendingSnapshot.Metadata.Index {
				return term, ErrCompacted
			}
		}
		return term, err
	}
	/*index := l.indexArray(i)
	if index >= len(l.entries) {
		return 0, ErrUnavailable
	}
	return l.entries[index].Term, nil*/

	/*if len(l.entries) > 0 && i >= l.FirstIndex {
		return l.entries[i-l.FirstIndex].Term, nil
	}
	term, err := l.storage.Term(i)
	return term, err*/

}

// ????
/*func (l *RaftLog) indexArray(i uint64) int {
	//return int(i - l.headEntry.Index - 1)
	return int(i - 1)
}*/
func (l *RaftLog) indexArray(i uint64) int {
	idx := int(i - l.FirstIndex)
	if idx < 0 {
		panic("indexArray: index < 0")
	}
	return idx
}

func (l *RaftLog) indexEntry(i int) uint64 {
	return uint64(i) + l.FirstIndex
}
