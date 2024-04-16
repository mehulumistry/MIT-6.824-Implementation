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
	"github.com/mehulumistry/MIT-6.824-Implementation/pkg/labrpc"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"
)

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type ServerRole int32

const (
	LEADER ServerRole = iota
	CANDIDATE
	FOLLOWER
)

const (
	HEARTBEAT_INTERVAL   = 100 * time.Millisecond // Send heartbeat every 100ms (10 times per second),
	ELECTION_TIMEOUT_MAX = 1000 * time.Millisecond
	ELECTION_TIMEOUT     = 150 * time.Millisecond
)

type Log struct {
	LogIndx int
	Message string
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	killCh         chan struct{}
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// static throughout, it'll only change if you are implementing membership change.
	quorumSize int

	// dynamic vars, don't forget to reset
	serverRole ServerRole

	// persisted state on all servers
	currentTerm int
	votedFor    int // candidateId that receive vote on current term
	log         []Log

	commitIndex int
	lastApplied int

	// leaders
	nextIndex  []int
	matchIndex []int
}

func randomizeElectionTimeout() time.Duration {
	return ELECTION_TIMEOUT + (time.Duration(rand.Int63()) % ELECTION_TIMEOUT)
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randomizeElectionTimeout())
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.heartbeatTimer.Stop()
	rf.heartbeatTimer.Reset(HEARTBEAT_INTERVAL)
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.serverRole == LEADER
}

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
}

func (rf *Raft) sendHeartbeats() {
	for {
		select {
		case <-rf.killCh:
			// Handle the case where the Raft instance is being stopped
			return
		case <-rf.heartbeatTimer.C:
			rf.rpcCallForHeartBeat()
		}
	}
}

func (rf *Raft) rpcCallForHeartBeat() {

	for indx, peer := range rf.peers {
		if rf.me == indx {
			continue
		} else {
			rf.resetHeartbeatTimer() // resetting the heartbeat

			go func(index int, p *labrpc.ClientEnd) {

				rf.mu.Lock()
				isLeader := rf.serverRole == LEADER

				if !isLeader {
					rf.mu.Unlock()
					return
				}
				appendEntriesHeartBeatArgs := AppendEntriesArgs{
					Term:       rf.currentTerm,
					LeaderId:   rf.me,
					LogEntries: []Log{},
				}
				rf.mu.Unlock()

				var appendEntriesHeartBeatReply AppendEntriesReply
				ok := p.Call("Raft.AppendEntries", &appendEntriesHeartBeatArgs, &appendEntriesHeartBeatReply)

				if ok {
					rf.mu.Lock()
					if appendEntriesHeartBeatReply.Term > rf.currentTerm {
						rf.updateToFollowerRoleAndCatchUpNewTerm(appendEntriesHeartBeatReply.Term)
						rf.resetElectionTimer()
					}
					rf.mu.Unlock()
				}
			}(indx, peer)
		}
	}

}

// CondInstallSnapshot A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LogEntries   []Log
	LeaderCommit int
}

// AppendEntriesReply for heartbeat and to replicate log entries
type AppendEntriesReply struct {
	Term    int // currentTerm, for candidate to update itself.
	Success bool
}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
// Invoked by candidates to collect votes
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int // currentTerm, for candidate to update itself.
	VoteGranted bool
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm := rf.currentTerm
	//DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[ELECTION_TIME_RESET], [Term: %d] AND TIME: %s, ELECTION TIMEOUT: %d", rf.currentTerm)
	reply.Term = currentTerm
	reply.VoteGranted = false

	// If the current term is higher, reject the request vote
	if currentTerm > args.Term {
		DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Current term is higher %d, rejecting the RequestVote for candidate %d and term: %d\n", rf.currentTerm, args.CandidateId, args.Term)
		return
	} else if currentTerm < args.Term {
		// if the request has higher term, it means you have a stale data. Update your term and change to follower.
		// You are allowed to vote in the new term.
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = args.Term
		rf.updateToFollowerRoleAndCatchUpNewTerm(args.Term)
		DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[@@@VOTE_GRANTED@@@] to candidateId %d, and term: %d", rf.votedFor, args.Term)
	} else if args.Term == currentTerm {
		if rf.serverRole == LEADER {
			return
		} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Already voted for candidate %d, rejecting RequestVote for candidate %d and term: %d", rf.votedFor, args.CandidateId, args.Term)
			return
		}
	}

	return
}

// AppendEntries RequestVote example RequestVote RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm := rf.currentTerm

	//DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[ELECTION_TIME_RESET], [Term: %d] AND TIME: %s, ELECTION TIMEOUT: %d", rf.currentTerm, rf.lastActivityTime, rf.electionTimeout)
	reply.Term = currentTerm

	// Process HeartBeats....
	if len(args.LogEntries) == 0 {

		if currentTerm < args.Term {
			reply.Term = args.Term
			rf.updateToFollowerRoleAndCatchUpNewTerm(args.Term)
			DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Got heartbeat from the new term: [%d], leader %d\n", rf.currentTerm, args.LeaderId)
		}
	}

	rf.resetElectionTimer()
	return
}

func (rf *Raft) startElection() {

	rf.mu.Lock()

	if rf.serverRole == LEADER {
		rf.mu.Unlock()
		return
	}

	rf.resetElectionTimer()
	rf.updateToCandidateRole()
	currentTerm := rf.currentTerm

	rf.mu.Unlock()

	requestVoteArgs := RequestVoteArgs{
		Term:        currentTerm,
		CandidateId: rf.me,
	}

	voteCh := make(chan bool, len(rf.peers))

	// Request votes in parallel
	for indx := range rf.peers {
		if rf.me == indx {
			continue
		}

		go func(peerIndex int) {
			for {
				select {
				case <-rf.killCh:
					return
				default:
					DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[RequestVote] %d ---> %d [Term: %d]", rf.me, peerIndex, rf.currentTerm)
					var requestVoteReply RequestVoteReply
					ok := rf.sendRequestVote(peerIndex, &requestVoteArgs, &requestVoteReply)

					if ok {
						rf.mu.Lock()
						if requestVoteReply.Term > rf.currentTerm {
							rf.updateToFollowerRoleAndCatchUpNewTerm(requestVoteReply.Term)
							rf.resetElectionTimer()
							voteCh <- false
						} else if requestVoteReply.VoteGranted && rf.currentTerm == requestVoteReply.Term {
							voteCh <- true
						} else {
							voteCh <- false
						}
						rf.mu.Unlock()
						return
					}
				}
			}
		}(indx)
	}

	// Collect votes and decide
	grantedVotes := 1 // Start with self-vote
	for i := 1; i < len(rf.peers); i++ {
		if <-voteCh {
			grantedVotes++

			if grantedVotes >= rf.quorumSize {
				rf.mu.Lock()

				if rf.currentTerm == currentTerm && rf.serverRole == CANDIDATE {
					rf.serverRole = LEADER
					rf.mu.Unlock()
					DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Got enough vote %d [Term: %d]", grantedVotes, rf.currentTerm)
					DPrintf("************* IAM THE LEADER %d**************", rf.me)
					go rf.sendHeartbeats()
				}
				return
			}
		}
	}

	rf.mu.Lock()
	if rf.serverRole == CANDIDATE {
		rf.stepDownToFollowerSameTerm()
	}
	rf.mu.Unlock()

}

func serverRoleToStr(role ServerRole) string {
	switch role {
	case LEADER:
		return "LEADER"
	case CANDIDATE:
		return "CANDIDATE"
	case FOLLOWER:
		return "FOLLOWER"
	default:
		return "Unknown Role"
	}
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
// handler function on the server side does not return.  Thus, there
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

// Start the service using Raft (e.g. a k/v server) wants to start
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	close(rf.killCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {

	for {
		select {
		case <-rf.killCh: // Check if context is cancelled
			return // Exit the loop if Raft instance is killed
		case <-rf.electionTimer.C:
			go rf.startElection()
		}
	}
}

// Make the service or tester wants to create a Raft server.
// the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order.
//
// persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any.
//
// applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func (rf *Raft) updateToFollowerRoleAndCatchUpNewTerm(term int) {
	DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "CATCH UP NEW TERM %s ---> FOLLOWER [Term: %d][NewTerm: %d] \n", serverRoleToStr(rf.serverRole), rf.currentTerm, term)
	rf.votedFor = -1
	rf.serverRole = FOLLOWER
	rf.currentTerm = term
}
func (rf *Raft) stepDownToFollowerSameTerm() {
	DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "STEP DOWN FROM %s ---> FOLLOWER [Term: %d][NewTerm: %d] \n", serverRoleToStr(rf.serverRole), rf.currentTerm, rf.currentTerm)
	rf.votedFor = -1
	rf.serverRole = FOLLOWER
}
func (rf *Raft) updateToCandidateRole() {
	rf.serverRole = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.quorumSize = (len(peers) / 2) + 1
	rf.electionTimer = time.NewTimer(randomizeElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(HEARTBEAT_INTERVAL)
	rf.killCh = make(chan struct{})
	rf.updateToFollowerRoleAndCatchUpNewTerm(0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
