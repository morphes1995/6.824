package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/src/labgob"
	"bytes"
	"math/rand"
	"sync"
	"time"
)
import "6.824/src/labrpc"

// import "bytes"
// import "labgob"

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// LogEntry each entry contains command for state machine,
// and term when entry was received by leader
type LogEntry struct {
	LogIndex int
	Term     int
	Command  interface{}
}

const (
	STATE_CANDIDATE = iota
	STATE_FOLLOWER
	STATE_LEADER
)

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state     int // current role
	voteCount int

	// Persistent state on all servers:
	currentTerm int /* latest term server has seen (initialized to 0 on first boot, increases monotonically)*/
	votedFor    int /* candidateId that received vote in current term (-1 if none) */
	logs        []LogEntry

	// Volatile state on all servers.
	commitIndex int /* index of the highest log entry known to be committed (initialized to 0, increases monotonically) */
	lastApplied int /* index of the highest log entry applied to state machine (initialized to 0, increases monotonically)  */

	// Volatile state on leader
	nextIndex  []int /*  for each server, index of the next log entry to send to that server (initialized to leader last log index + 1) */
	matchIndex []int /* known to be replicated on server (initialized to 0, increases monotonically) */

	// Channels between raft peers.
	grantVoteC   chan bool
	winElectionC chan bool
	heartBeatC   chan bool

	discoverHigherTermC chan bool
	killC               chan bool

	applyC chan ApplyMsg
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == STATE_LEADER
	rf.mu.Unlock()

	return term, isleader
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {

		DPrintf("error while decode data")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int /* candidate’s term */
	CandidateId  int /* Id of the candidate that requesting vote */
	LastLogIndex int /* index of candidate’s last log entry */
	LastLogTerm  int /* term of candidate’s last log entry */

}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  /* latest currentTerm, for candidate to update itself */
	VoteGranted bool /* true means candidate received vote */
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("%d receive vote request from %d\n", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reject candidate's vote request with stale term number
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// if leader/candidate/follower discovers server with higher term, reset to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.voteCount = 0
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm

	reply.VoteGranted = false
	// grant vote only if
	// 1) votedFor is null or candidateId,
	// 2) and candidate’s log is at least as up-to-date as receiver’s log
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = true

		rf.votedFor = args.CandidateId
		rf.grantVoteC <- true
	}

	// state has changed
	rf.persist()
}

func (rf *Raft) isUpToDate(otherTerm int, otherIndex int) bool {
	latestTerm, latestIndex := rf.getLastLogTerm(), rf.getLastLogIndex()
	if otherTerm > latestTerm || (otherTerm == latestIndex && otherIndex >= otherIndex) {
		return true
	}
	return false
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("%d send vote request to %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// for cases
	//  1) found newer term in RequestVoteReply, or in dealing of RequestVote ,
	//     and already made this candidate reset to follower, discard this reply
	//  2) previous reply already voted this candidate as leader, discard this reply
	//  3) this reply consume a long transfer time , and this candidate's election timeout reached
	//     and then became candidate again with currentTerm+1 , discard this reply
	if rf.state != STATE_CANDIDATE || rf.currentTerm != args.Term {
		return ok
	}

	// candidate discovers term newer than itself , reset to follower
	if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.state = STATE_FOLLOWER
		rf.voteCount = 0
		rf.votedFor = -1
		// state has changed
		rf.persist()
		rf.discoverHigherTermC <- true
		return ok
	}

	if reply.VoteGranted {
		rf.voteCount++
		// candidate has been received votes from the majority of servers
		if rf.voteCount > len(rf.peers)/2 {
			rf.initNewLeaderSafe()
			rf.winElectionC <- true
			DPrintf("%d become leader\n", rf.me)
		}
	}

	return ok
}

func (rf *Raft) initNewLeaderSafe() {
	rf.state = STATE_LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	nextLogIdx := rf.getLastLogIndex() + 1
	for i, _ := range rf.peers {
		rf.nextIndex[i] = nextLogIdx // initialized to leader last log index + 1)
	}
}

func (rf *Raft) broadcastRequestVote() {
	DPrintf("%d broadcast request vote\n", rf.me)
	args := &RequestVoteArgs{}
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()
	rf.mu.Unlock()

	for i, _ := range rf.peers {
		if (i != rf.me) && (rf.state == STATE_CANDIDATE /*  rf.state may already become follower because the discovery of higher term  */) {
			go rf.sendRequestVote(i, args, &RequestVoteReply{})
		}
	}
}

type AppendEntriesArgs struct {
	Term         int        /* leader’s term */
	LeaderId     int        /* so follower can redirect clients */
	PrevLogIndex int        /* index of log entry immediately preceding new ones */
	PrevLogTerm  int        /* term of prevLogIndex entry */
	Entries      []LogEntry /* log entries to store (empty for heartbeat; may send more than one for efficiency) */
	LeaderCommit int        /* leader’s commitIndex */
}

type AppendEntriesReply struct {
	Term             int  /* currentTerm, for leader to update itself (if this leader is stale )*/
	Success          bool /* true if follower contained entry matching prevLogIndex and prevLogTerm */
	ExceptedLogIndex int  /* index of log entry that  follower excepted */
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("%d receive append entries request from %d ,args.PrevLogIndex %d,lastLogIndex: %d, commitIdx %d, \n",
		rf.me, args.LeaderId, args.PrevLogIndex, rf.getLastLogIndex(), rf.commitIndex)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reject leader's heartbeat with stale term number
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	//  1) if leader/candidate/follower discovers server with higher term, reset to follower
	//  2) if candidate receives a heartbeat from current term leader , then reset to follower
	if args.Term > rf.currentTerm || (rf.state == STATE_CANDIDATE && args.Term == rf.currentTerm) {
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.voteCount = 0
		rf.votedFor = -1

		// state has changed
		rf.persist()
	}

	rf.heartBeatC <- true

	reply.Term = rf.currentTerm

	if args.PrevLogIndex > rf.getLastLogIndex() {
		// 1. logs[PrevLogIndex] in this follower didn't exist
		reply.Success = false
		reply.ExceptedLogIndex = rf.getLastLogIndex() + 1
	} else if myPrevLogTerm := rf.logs[args.PrevLogIndex].Term; myPrevLogTerm != args.PrevLogTerm { // PrevLogIndex always > 0
		// 2. logs[PrevLogIndex].term mismatch
		reply.Success = false
		reply.ExceptedLogIndex = args.PrevLogIndex //todo optimize ??
	} else {
		// 3. use leader's log entries to overwritten my corresponding part of log entries
		rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...) //todo optimize ??
		reply.Success = true
		reply.ExceptedLogIndex = args.PrevLogIndex + len(args.Entries) + 1

		// state has changed
		rf.persist()
	}

	DPrintf("%d finish append entries request from %d ,ExceptedLogIndex: %d, success %v, \n",
		rf.me, args.LeaderId, reply.ExceptedLogIndex, reply.Success)
	if !reply.Success {
		return
	}
	// if the logs caught up with leader , then sync commitIndex with leader
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit // follower committed
		go rf.applyLog()
	}
}

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyC <- ApplyMsg{CommandIndex: i, CommandValid: true, Command: rf.logs[i].Command}
	}
	rf.lastApplied = rf.commitIndex // follower applied
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("%d send append entries request to %d, nextIdx %d, leaderCommitIdx: %d \n",
		rf.me, server, args.PrevLogIndex+1, args.LeaderCommit)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// for cases
	//  1) found newer term in AppendEntriesReply, or in dealing of RequestVote ,
	//     and already reset this leader to follower, discard this reply
	//  2) if leader reset to follower , and then election timeout, this leader become candidate, discard this reply
	//  3) this reply consume a long transfer time ,if leader reset to follower, and then election timeout, this leader become candidate
	//     , and then be voted to leader again with currentTerm+1 , discard this reply
	if rf.state != STATE_LEADER || rf.currentTerm != args.Term {
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
		rf.voteCount = 0

		// state has changed
		rf.persist()

		rf.discoverHigherTermC <- true
		DPrintf("%d leader find a newer term %d from %d", rf.currentTerm, reply.Term, server)
		return ok
	}

	rf.nextIndex[server] = reply.ExceptedLogIndex
	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
	}

	// forward commitIndex
	// If there exists an i such that i > commitIndex, and a majority of
	// matchIndex[peer] >= i, and log[N].term == currentTerm (only commit log entries of current term)
	// set commitIndex = i
	majority := len(rf.peers)/2 + 1
	for i := rf.getLastLogIndex(); i > rf.commitIndex; i-- {
		if rf.logs[i].Term < rf.currentTerm {
			break // do not commit logs of other term
		}
		count := 0
		for peer, _ := range rf.peers {
			if rf.matchIndex[peer] >= i {
				count++
			}
		}

		if count >= majority {
			rf.commitIndex = i
			DPrintf("leader %d ,forward commitIndex to  %d, matchIndex: %v", rf.me, rf.commitIndex, rf.matchIndex)
			go rf.applyLog()
			break
		}
	}
	DPrintf("leader %d , matchIndex: %v", rf.me, rf.matchIndex)
	return ok
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i != rf.me && rf.state == STATE_LEADER /*  rf.state may already become follower because the discovery of higher term  */ {
			args := &AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.LeaderCommit = rf.commitIndex

			args.PrevLogIndex = rf.nextIndex[i] - 1
			if args.PrevLogIndex >= 0 {
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			}

			// entries need be sent to followers
			if rf.nextIndex[i] <= rf.getLastLogIndex() {
				args.Entries = rf.logs[rf.nextIndex[i]:]
			}

			go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
		}
	}
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := 0
	term := 0
	isLeader := rf.state == STATE_LEADER

	if isLeader {
		term = rf.currentTerm
		index = rf.getLastLogIndex() + 1
		rf.logs = append(rf.logs, LogEntry{
			Term:     term,
			LogIndex: index,
			Command:  command,
		})
		// state has changed
		rf.persist()

		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		DPrintf("add log entry to leader %d ,current logs len : %d", rf.me, len(rf.logs))
		// broadcastAppendEntries groutine will send to followers later
	}

	return index, term, isLeader
}

// Kill the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.killC <- true
}

func (rf *Raft) Run() {
	for {

		select {
		case <-rf.killC:
			DPrintf("%d raft main loop exited , %p", rf.me, rf)
			return
		default:
		}

		switch rf.state {
		case STATE_FOLLOWER:
			select {
			// 1) receive any valid heartbeat from current leader or valid vote request
			// 2) receive any valid vote request from candidate
			// then reset election timeout in next loop
			case <-rf.heartBeatC:
			case <-rf.grantVoteC:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(150)+200)): // not receive any valid heartbeat or valid vote request , election timeout!!
				DPrintf("%d become candidate, %p\n", rf.me, rf)
				rf.mu.Lock()
				rf.state = STATE_CANDIDATE
				rf.mu.Unlock()
			}

		case STATE_LEADER:
			go rf.broadcastAppendEntries()

			select {
			case <-rf.discoverHigherTermC: // find a newer term ,convert to follower
			case <-time.After(time.Millisecond * 100): // heartbeat timeout
			}

		case STATE_CANDIDATE:
			rf.mu.Lock()
			rf.votedFor = rf.me
			rf.currentTerm += 1
			rf.voteCount = 1

			// state has changed
			rf.persist()

			rf.mu.Unlock()
			go rf.broadcastRequestVote()

			select {
			case <-rf.discoverHigherTermC: // find a newer term ,convert to follower
			case <-rf.heartBeatC: // receive appendEntries from a valid leader , become follower
			case <-rf.winElectionC: // receive majority votes, become leader and broadcast append entries in next loop
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(150)+200)): // vote split, go to next election in next loop
				DPrintf("%d candidate vote split ,vote: %d\n", rf.me, rf.voteCount)
			}
		}
	}
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	DPrintf("make raft instance %d %p", me, rf)
	rf.mu = sync.Mutex{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyC = applyCh

	// Your initialization code here (2A, 2B, 2C).

	rf.state = STATE_FOLLOWER
	rf.voteCount = 0

	prevState := persister.ReadRaftState()
	if prevState == nil || len(prevState) < 1 {
		// first bootstrap
		rf.votedFor = -1
		rf.currentTerm = 0
		// at least one log entry
		rf.logs = append(rf.logs, LogEntry{0, 0, nil})
		rf.persist()
	} else {
		// initialize from state persisted before a crash
		rf.readPersist(prevState)
		DPrintf("%d read from persister, %p", rf.me, rf)
	}

	rf.commitIndex = 0
	rf.lastApplied = 0

	// init as non-buffered chan
	rf.grantVoteC = make(chan bool, 100)
	rf.winElectionC = make(chan bool, 100)
	rf.heartBeatC = make(chan bool, 100)

	rf.discoverHigherTermC = make(chan bool, 100)
	rf.killC = make(chan bool)

	go rf.Run()

	return rf
}
