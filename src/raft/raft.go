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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"labgob"
	"labrpc"
)

// ==========================================================
// 1. Definitions
// ==========================================================

// as each Raft peer becomes aware that successive log entries are
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

type NodeRole int

const (
	Leader NodeRole = iota
	Follower
	Candidate
)

const HeartBeatTimeout = 100 * time.Millisecond

func GetRandomElectionTimeout() time.Duration {
	ms := 300 + rand.Intn(200)
	return time.Duration(ms) * time.Millisecond
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile
	role        NodeRole
	commitIndex int // the highest log entry known to be committed
	lastApplied int // the highest log entry applied

	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	electionTimer  *time.Timer
	heartBeatTimer *time.Timer
}

// ==========================================================
// 2. Lifecycle & Public API
// ==========================================================

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := (rf.role == Leader)

	return term, isleader
}

// the tester calls Kill() when a Raft instance won't be needed again.
// you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

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

	if rf.role != Leader {
		return -1, -1, false
	}

	term := rf.currentTerm
	entry := LogEntry{
		Command: command,
		Term:    term,
	}
	rf.log = append(rf.log, entry)
	rf.persist()
	index := rf.getLastLogIndex()
	go rf.BroadcastHeartbeat()

	return index, term, true
}

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

	// Seed the random number generator to ensure different election timeouts
	rand.Seed(time.Now().UnixNano())

	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,
		applyCh:   applyCh,

		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 1),

		role:        Follower,
		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),

		electionTimer:  time.NewTimer(GetRandomElectionTimeout()),
		heartBeatTimer: time.NewTimer(HeartBeatTimeout),
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.applier()

	return rf
}

// ==========================================================
// 3. Helpers
// ==========================================================

// Get the index of the last log entry
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

// Get the term of the last log entry
func (rf *Raft) getLastLogTerm() int {
	return rf.log[rf.getLastLogIndex()].Term
}

// ==========================================================
// 4. Persistence
// ==========================================================

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		DPrintf("error decoding")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// ==========================================================
// 5. Background Loops
// ==========================================================

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			isLeader := (rf.role == Leader)
			rf.mu.Unlock()

			rf.electionTimer.Reset(GetRandomElectionTimeout())
			if !isLeader {
				rf.StartElection()
			}

		case <-rf.heartBeatTimer.C:
			rf.mu.Lock()
			isLeader := (rf.role == Leader)
			rf.mu.Unlock()

			rf.heartBeatTimer.Reset(HeartBeatTimeout)
			if isLeader {
				go rf.BroadcastHeartbeat()
			}
		}
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()

		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.log[lastApplied+1:commitIndex+1])
		rf.lastApplied = max(commitIndex, lastApplied)
		rf.mu.Unlock()

		for i, entry := range entries {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: lastApplied + 1 + i,
			}
			rf.applyCh <- msg
		}
	}
}

// ==========================================================
// 6. Leader Election (RequestVote)
// ==========================================================

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
	}

	upToDate := false
	myLastIndex := rf.getLastLogIndex()
	myLastLogTerm := rf.getLastLogTerm()

	if args.LastLogTerm > myLastLogTerm {
		upToDate = true
	} else if args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastIndex {
		upToDate = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.Term, reply.VoteGranted = rf.currentTerm, true
		rf.electionTimer.Reset(GetRandomElectionTimeout())
		return
	}

	reply.Term, reply.VoteGranted = rf.currentTerm, false
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	me := rf.me
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = me
	rf.persist()

	votes := 1
	request := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == me {
			continue
		}

		go func(peer int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(peer, request, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.role != Candidate || rf.currentTerm != request.Term {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = Follower
					rf.votedFor = -1
					rf.persist()
					return
				}

				if reply.VoteGranted {
					votes++
					if votes == len(rf.peers)/2+1 {
						DPrintf("[Election] %d becomes leader", me)
						rf.role = Leader
						for i := range len(rf.peers) {
							rf.nextIndex[i] = rf.getLastLogIndex() + 1
							rf.matchIndex[i] = 0
						}
						go rf.BroadcastHeartbeat()
					}
				}
			}
		}(peer)
	}
}

// ==========================================================
// 7. Log Replication & Heartbeats (AppendEntries)
// ==========================================================

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	rf.electionTimer.Reset(GetRandomElectionTimeout())
	rf.role = Follower
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > rf.getLastLogIndex() { // empty
		reply.Term, reply.Success = rf.currentTerm, false
		reply.ConflictIndex, reply.ConflictTerm = len(rf.log), -1
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // term doesn't match
		reply.Term, reply.Success = rf.currentTerm, false

		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i > 0; i-- {
			if rf.log[i].Term != reply.ConflictTerm {
				reply.ConflictIndex = i + 1
				break
			}
		}

		rf.log = rf.log[:args.PrevLogIndex]
		rf.persist()
		return
	}

	for i, entry := range args.Entries {
		curr_index := args.PrevLogIndex + 1 + i

		if curr_index > rf.getLastLogIndex() { // empty
			rf.log = append(rf.log, entry)
		} else if rf.log[curr_index].Term != entry.Term { // conflict
			rf.log = rf.log[:curr_index]
			rf.log = append(rf.log, entry)
		}
	}
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) BroadcastHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		entries := make([]LogEntry, 0)
		nextIndex := rf.nextIndex[peer]
		if nextIndex <= rf.getLastLogIndex() {
			entries = append(entries, rf.log[nextIndex:]...)
		}

		request := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.log[nextIndex-1].Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		go func(peer int, request *AppendEntriesArgs) {
			reply := new(AppendEntriesReply)
			if rf.sendAppendEntries(peer, request, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.role != Leader || rf.currentTerm != request.Term {
					return
				}

				if reply.Success {
					rf.matchIndex[peer] = request.PrevLogIndex + len(request.Entries)
					rf.nextIndex[peer] = rf.matchIndex[peer] + 1

					for N := rf.getLastLogIndex(); N > rf.commitIndex; N-- {
						if rf.log[N].Term == rf.currentTerm {
							count := 1
							for i := range rf.peers {
								if rf.matchIndex[i] >= N {
									count++
								}
							}
							if count > len(rf.peers)/2 {
								rf.commitIndex = N
								break
							}
						}
					}
				} else {
					// there is another new leader
					if reply.Term > rf.currentTerm {
						rf.role = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
						return
					}
					// log inconsistency
					if reply.ConflictTerm == -1 {
						rf.nextIndex[peer] = reply.ConflictIndex
					} else {
						rf.nextIndex[peer] = reply.ConflictIndex
						for i := rf.getLastLogIndex(); i >= 0; i-- {
							if rf.log[i].Term == reply.ConflictTerm {
								rf.nextIndex[peer] = i + 1
								break
							}
						}
					}
				}
			}
		}(peer, request)
	}
}
