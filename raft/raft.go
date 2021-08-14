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
	"errors"
	//"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set WHEN RESTARTING
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	// the term for the leader
	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	//*********************
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int

	// MY VARIETY , current baseline of election interval
	// To avoid splitting up the vote
	// range in [electionTimeout, 2 * electionTimeout]
	currentElectionTimeout int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	transferring bool

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	stableState, confState, _ := c.Storage.InitialState()
	raft := &Raft{
		id:   				c.ID,
		Term: 				stableState.GetTerm(), //???
		Vote: 				stableState.GetVote(),

		RaftLog:			newLog(c.Storage),
		Prs: 				make(map[uint64]*Progress),
		State: 				StateFollower,
		votes: 				make(map[uint64]bool),
		//msgs: ,
		Lead: 				None,

		//To avoid splitting up the vote
		electionTimeout: 	c.ElectionTick ,
		currentElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
		//electionElapsed: 	0,
		heartbeatTimeout: 	c.HeartbeatTick,
		//heartbeatElapsed: 	0,
		leadTransferee: 0,
		PendingConfIndex: 0,
		transferring: false,
	}
	li, _ := c.Storage.LastIndex()
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	for _, peer := range c.peers {
		//fmt.Println(i)
		if peer == raft.id {
			raft.Prs[peer] = &Progress{
				Match: li,
				Next:  li + 1,
			}
		} else {
			raft.Prs[peer] = &Progress{
				Match: 0,
				Next:  li + 1,
			}
		}
	}
	if c.Applied > 0 {
		raft.RaftLog.applied = c.Applied
	}
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// In 'sendAppend', if a leader fails to get term or entries,
	//the leader requests snapshot by sending
	// 'MessageType_MsgSnapshot' type message.

	li := r.RaftLog.LastIndex()
	ni := r.Prs[to].Next
	logterm, err := r.RaftLog.Term(ni - 1)
	if err != nil { //?
		if err == ErrCompacted{
			r.sendSnapshot(to)
			//panic(err)
			return false
		}else{
			panic(err)
		}

	}
	entries := make([]*pb.Entry, 0, li - ni + 1)

	for i := r.RaftLog.indexArray(ni); i < len(r.RaftLog.entries); i++{
		entries = append(entries,&r.RaftLog.entries[i])
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From: r.id,
		To: to,
		Term: r.Term,
		LogTerm: logterm,
		Index: ni - 1,
		Entries: entries,
		Commit: r.RaftLog.committed,
	}
	r.msgs = append(r.msgs,msg)
	return true
}

func (r *Raft) sendAppendResponse(to uint64, reject bool, index uint64, len int, logterm uint64){
	//logterm, _ := r.RaftLog.Term(index)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From: r.id,
		To: to,
		Term: r.Term,
		LogTerm: logterm,
		Reject: reject,
		Index: index + uint64(len),
	}
	r.msgs = append(r.msgs,msg)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From: r.id,
		To: to,
		Term: r.Term,
		LogTerm: 0,
		Index: 0,
		Commit: r.RaftLog.committed,
	}
	r.msgs = append(r.msgs,msg)
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool, index uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: r.RaftLog.LastTerm(),
		Commit: r.RaftLog.committed,
		Reject:  reject,
		Index:   index,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendTimeoutNow(to uint64) {
	// maybe in 3A
	msg := pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		From:    r.id,
		To:      to,
	}
	r.msgs = append(r.msgs, msg)
}
func (r *Raft) sendSnapshot(to uint64){
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil{
		// Snapshot returns the most recent snapshot.
		// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
		// so raft state machine could know that Storage needs some time to prepare
		// snapshot and call Snapshot later.
		return
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgSnapshot,
		From:    r.id,
		To:      to,
		Snapshot: &snapshot,
	}
	r.msgs = append(r.msgs, msg)
	//if snapshot == nil{
		//panic(ErrUnavailable)
	//}
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}




func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	// Your Code Here (2A).
	if !reject{
		r.Vote = to
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From: r.id,
		To: to,
		Term: r.Term,
		Commit: r.RaftLog.committed,
		Reject: reject,
	}
	r.msgs = append(r.msgs,msg)
}

func (r *Raft) bcastAppend() {
	for peer, _ := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

// tick advances the internal logical clock by a single tick.
// term : logical tick
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State{
	case StateFollower:
		r.electionElapsed++
		if r.electionElapsed >= r.currentElectionTimeout{
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	case StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.currentElectionTimeout{// to start a new round of election(the 3th possibility in election)
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout{
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.electionElapsed = 0

	//election timeout randomized
	r.currentElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.electionElapsed = 0
	//election timeout randomized
	r.currentElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Vote = r.id
	r.votes = make(map[uint64]bool,len(r.Prs))
	//candidate should vote for itself
	r.votes[r.id] = true
	r.Term ++
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term

	r.State = StateLeader
	r.Lead = r.id
	r.RaftLog.entries = append(r.RaftLog.entries,pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term: r.Term,
		Index: r.RaftLog.LastIndex() + 1,
	},
	)

	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	li := r.RaftLog.LastIndex()
	for peer, _ := range r.Prs{
		//When a leader first comes to power,
		//it initializes all nextIndex values to the index just after the
		//last one in its log


		if peer != r.id {
			r.Prs[peer] = &Progress{
				Next : li,
			}

			// calls bcastAppend
			r.sendAppend(peer)
		} else {
			r.Prs[peer] = &Progress{
				Match: li,
				Next : li + 1,
			}
		}
	}
	//?

}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	_, ok := r.Prs[r.id]
	if m.MsgType == pb.MessageType_MsgTimeoutNow && !ok {
		// when a MessageType_MsgTimeoutNow arrives at
		// a node that has been removed from the group, nothing happens
		return nil
	}
	//Can be merged!!!
	switch r.State {
	case StateFollower:
		switch m.MsgType{
		case pb.MessageType_MsgHup:
			r.campaign()
		case pb.MessageType_MsgPropose:
			//When passed to follower, 'MessageType_MsgPropose'
			//is stored in follower's mailbox(msgs) by the send
			//	method. It is stored with sender's ID and later
			//	forwarded to the leader by
			//	rafthttp package.
			m.From = r.id
			m.To = r.Lead
			r.msgs = append(r.msgs,m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgTimeoutNow:
			//after receiving a MsgTimeoutNow message
			// the transferee should start a new election immediately
			// regardless of its election timeout
			r.campaign()
		case pb.MessageType_MsgTransferLeader:
			if r.Lead != None{
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		}
	case StateCandidate:
		switch m.MsgType{
		case pb.MessageType_MsgHup:
			r.campaign()
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgTransferLeader:
			if r.Lead != None{
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		}
	case StateLeader:
		switch m.MsgType{
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgBeat:
			for peer,_ := range r.Prs{
				if peer != r.id{
					r.sendHeartbeat(peer)
				}
			}
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
		case pb.MessageType_MsgPropose:
			// If the transferee’s log is not up to date,
			// the current leader should send a MsgAppend message
			// to the transferee and stop accepting new proposals
			// in case we end up cycling
			if !r.transferring{
				r.appendEntry(m)
			}

		case pb.MessageType_MsgTransferLeader:
			r.handleTransferLeader(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		}
	}
	return nil
}

//handleHup handle Hup RPC request
// to start a new election
// for Candidate and Follower
func (r *Raft) campaign() {
	// Your Code Here (2A).
	r.becomeCandidate()
	//if there's only one, he'll become leader immediately
	//or there will be no leader
	if len(r.Prs) == 1{
		r.becomeLeader()
		return
	}
	r.Vote = r.id
	r.votes[r.id] = true
	//r.sendRequestVoteResponse(r.id, false) //vote for itself
	for peer, _ := range r.Prs{
		if peer != r.id{ //send vote request to peers in cluster
			msg := pb.Message{
				MsgType: 	pb.MessageType_MsgRequestVote,
				To:		 	peer,
				From:    	r.id,
				Term:    	r.Term,
				LogTerm: 	r.RaftLog.LastTerm(),
				Index:   	r.RaftLog.LastIndex(),
			}
			r.msgs = append(r.msgs, msg)
		}
	}
}

//handleRequestVote handle RequestVote RPC request
// for Candidate and Follower and Leader
func (r *Raft) handleRequestVote(m pb.Message) {
	// Your Code Here (2A).
	if r.State != StateFollower && m.Term <= r.Term{
		r.sendRequestVoteResponse(m.From, true)
	}else{
		//If leader or candidate receives 'MessageType_MsgRequestVote' with higher term, it will revert
		//	back to follower
		// because the leader or the candidate is out of date
		if r.State != StateFollower{
			r.becomeFollower(r.Term, None)
		}
		lastTerm := r.RaftLog.LastTerm() // ?
		li := r.RaftLog.LastIndex()
		// the voter denies its vote if its own log is more up-to-date
		// than that of the candidate.
		if lastTerm < m.LogTerm || lastTerm == m.LogTerm && li <= m.Index {
			// each follower will vote for at most one
			// candidate in a given term, on a first-come-first-served basis.
			if r.Term == m.Term && (r.Vote == None || r.Vote == m.From) {
				r.sendRequestVoteResponse(m.From, false)
			}else if r.Term != m.Term{
				// Voting in another term doesn't
				// affect voting in this term
				r.Term = m.Term
				r.sendRequestVoteResponse(m.From, false)
			} else{
				r.sendRequestVoteResponse(m.From, true)
			}
		}else {
			if m.Term > r.Term{
				r.Term = m.Term
			}
			r.sendRequestVoteResponse(m.From, true)
		}
	}
}

// handleRequestVoteResponse handle RequestVoteResponse RPC request
// for Candidate and Leader
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// Your Code Here (2A).
	r.votes[m.From] = !m.Reject
	count := 0
	for _, vote := range r.votes{
		if vote{
			count++
		}
	}

	if len(r.votes) - count > len(r.Prs) / 2{
		//If candidate receives majority of votes
		//of denials, it reverts back to follower.
		r.becomeFollower(r.Term,None)
	}else if float32(count) > float32(len(r.Prs)) / 2{
		r.becomeLeader()
	}
}

// handleAppendEntries handle AppendEntries RPC request
// for all of 3 states
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	//?????

	if m.Term < r.Term{
		//If a candidate or leader discovers that its term is out of date,
		// it immediately reverts to follower state.
		r.sendAppendResponse(m.From, true, None, 0, None)
		return
	}else {
		r.Lead = m.From
		//the AppendEntries consistency check
		/*if err != nil || logterm != m.LogTerm {
			r.sendAppendResponse(m.From, true, m.Index, len(m.Entries))
			return
		}*/
			if r.State != StateFollower{
				r.becomeFollower(m.Term, m.From)
			}
			r.Term = m.Term
			l := r.RaftLog
			lastIndex := l.LastIndex()
			if m.Index > lastIndex {
				r.sendAppendResponse(m.From, true, lastIndex+1, 0, None)
				return
			}else if m.Index >= l.FirstIndex {
				logTerm, err := l.Term(m.Index)
				if err != nil {
					panic(err)
				}
				if logTerm != m.LogTerm {
					index := l.indexEntry(sort.Search(l.indexArray(m.Index+1),
						func(i int) bool {
							return l.entries[i].Term == logTerm
						}))
					r.sendAppendResponse(m.From, true, index, 0, logTerm)
					return
				}
			}
			if m.Commit > r.RaftLog.committed {
				r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
			}
			maxIndex := uint64(0) // the max index in m.Entries,
			// for deleting  the existing conflict entry
			//when AppendEntries RPC is valid,
			// the follower will delete the existing
			//conflict entry and all that follow it

			for _, entry := range m.Entries{
				if entry.Index < l.FirstIndex {
					continue
				}
				index := entry.Index
				if index <= l.LastIndex(){ // change some log entries
					logTerm, err := l.Term(entry.Index)
					if err != nil {
						panic(err)
					}
					if logTerm != entry.Term{
						if index > maxIndex{
							maxIndex = index
						}
						idx := l.indexArray(entry.Index)
						r.RaftLog.entries[idx] = *entry
						r.RaftLog.stabled = min(index - 1, r.RaftLog.stabled)
					}
				}else{ // add entries only
					if index > maxIndex{
						maxIndex = index
					}
					r.RaftLog.entries = append(r.RaftLog.entries,*entry)
				}
			}

			//delete the existing conflict entry
			if maxIndex != 0 && maxIndex <= uint64(len(r.RaftLog.entries)){
				r.RaftLog.entries = r.RaftLog.entries[:maxIndex]
			}
			//if r.Lead == None{
			//r.Lead = m.From
			//}
			r.sendAppendResponse(m.From, false, m.Index, len(m.Entries), None)
		}



}

// handleAppendResponse handle AppendResponse RPC request
func (r *Raft) handleAppendResponse(m pb.Message) {
	// Your Code Here (2A).

	if m.Term != None && m.Term < r.Term {
		return
	}


	if m.Reject{
		index := m.Index
		if index == None{
			//If a candidate or leader discovers that its term is out of date,
			// it immediately reverts to follower state.
			r.becomeFollower(m.Term, None) //?????  to be debugged
		}else{
			// the case that Leader's entries' term does not
			// match follower's entries' term
			//the AppendEntries consistency check
			logTerm := m.LogTerm
			l := r.RaftLog
			// search for the suited index
			arrayIndex := sort.Search(len(l.entries),
				func(i int) bool {
					return l.entries[i].Term > logTerm
				})
			if arrayIndex > 0 && l.entries[arrayIndex-1].Term == logTerm {
				index = l.indexEntry(arrayIndex)
			}
			r.Prs[m.From].Next = index
			r.sendAppend(m.From)
		}
		return
	}else{
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1

		// a log entry is committed once the
		// leader that created the entry has replicated it
		// on a majority of the servers
		// ???if len(r.Prs) == 1
		logTerm, _ := r.RaftLog.Term(m.Index)
		if m.Index > 0 &&  r.Term != logTerm{
			return
		}
		count := 0
		for peer, progress := range r.Prs{
			if peer != r.id && progress.Match > r.RaftLog.committed{
				count++
			}
		}
		if count >= len(r.Prs) / 2{
			r.RaftLog.committed = max(m.Index, r.RaftLog.applied)
			r.bcastAppend() // update everyone's "commit"
		}

		if m.From == r.leadTransferee && m.Index == r.RaftLog.LastIndex() {
			r.sendTimeoutNow(m.From)
			r.leadTransferee = None
			//???
			r.transferring = false
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true, None)
	}else{
		if r.State == StateCandidate{
			r.becomeFollower(m.Term, m.From)
		}
		r.Lead = m.From
		r.electionElapsed = 0
		r.sendHeartbeatResponse(m.From, false, r.RaftLog.LastIndex())
	}
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// Your Code Here (2A).

	//leader can send log
	// to follower when it received a heartbeat response
	// which indicate it doesn't have update-to-date log
	if m.Reject{
		r.becomeFollower(m.Term, None)
	}
	if m.Commit < r.RaftLog.committed{
		r.sendAppend(m.From)
	}
}

func (r *Raft) appendEntry(m pb.Message) {
	// Your Code Here (2A).
	index := r.RaftLog.LastIndex()
	for i := 0; i < len(m.Entries); i++ {
		// ?????? r.PendingConfIndex
		entry := *m.Entries[i]
		index++
		entry.Term = r.Term
		entry.Index = index
		r.RaftLog.entries = append(r.RaftLog.entries, entry)
	}
	r.Prs[r.id] =
		&Progress{
			Match: r.RaftLog.LastIndex(),
			Next:  r.RaftLog.LastIndex() + 1,
		}


	//bcastAppend :send those entries to
	//	its peers.
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	r.bcastAppend()
	/*for peer, _ := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}*/

}

func (r *Raft) handleTransferLeader(m pb.Message) {
	//???
	/*if m.From == r.id {
		return
	}*/
	if r.leadTransferee != None && r.leadTransferee == m.From {
		return
	}
	if _, ok := r.Prs[m.From]; !ok {
		// Transfer leadership to non-existing node, there will be noop.
		return
	}
	r.leadTransferee = m.From
	//r.transferElapsed = 0
	if r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		// the current leader should first
		// check the qualification of the transferee
		r.sendTimeoutNow(m.From)
	} else {
		// If the transferee’s log is not up to date,
		// the current leader should send a MsgAppend message
		// to the transferee and stop accepting new proposals
		r.transferring = true
		r.sendAppend(m.From)
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	metadata := m.Snapshot.Metadata
	index := metadata.Index
	if index <= r.RaftLog.committed {
		r.sendAppendResponse(m.From, false, r.RaftLog.committed, 0, None)
		return
	}else{
		// initialize based on the Snapshot
		r.becomeFollower(max(r.Term, m.Term), m.From)
		r.RaftLog.entries = nil
		r.RaftLog.FirstIndex = index + 1
		r.RaftLog.applied = index
		r.RaftLog.committed = index
		r.RaftLog.stabled = index
		r.Prs = make(map[uint64]*Progress)
		for _, peer := range metadata.ConfState.Nodes{
			r.Prs[peer] = &Progress{}
		}
		r.RaftLog.pendingSnapshot = m.Snapshot
		r.sendAppendResponse(m.From, false, r.RaftLog.LastIndex(), 0, None)
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok{
		r.Prs[id] = &Progress{
			Match: 0,
			Next: 1, //?
		}
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok{
		delete(r.Prs, id)
		//?
		//logTerm, _ := r.RaftLog.Term(m.Index)
		//if m.Index > 0 &&  r.Term != logTerm{
			//return
		//}
		if r.State == StateLeader{
			if len(r.Prs) == 1{
				if r.Prs[r.id].Match > r.RaftLog.committed{
					r.RaftLog.committed = r.Prs[r.id].Match
					r.bcastAppend()
				}
			}else{
				count := 0
				for peer, progress := range r.Prs{
					if peer != r.id && progress.Match > r.RaftLog.committed{
						count++
					}
				}
				if count >= len(r.Prs) / 2{
					r.RaftLog.committed = r.RaftLog.applied
					r.bcastAppend() // update everyone's "commit"
				}
			}

		}
	}

}
