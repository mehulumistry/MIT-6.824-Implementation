package raft

// https://wenzhe.one/MIT6.824%2021Spring/Lab3.html
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
	"encoding/gob"
	"fmt"
	"github.com/mehulumistry/MIT-6.824-Implementation/pkg/labrpc"
	"github.com/sasha-s/go-deadlock"
	"math/rand"
	"sync/atomic"
	"time"
	//	"bytes"
	//"sync"
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
	HEARTBEAT_INTERVAL             = 100 * time.Millisecond // Send heartbeat every 100ms (10 times per second),
	ElectionTimeoutMin             = 300 * time.Millisecond
	ElectionTimeoutMax             = 600 * time.Millisecond
	APPLYING_LOGS_AND_PERSIST_FREQ = 10 * time.Millisecond
	RPC_CALLS_SLEEP                = 10 * time.Millisecond
	TICKER_CALLS_SLEEP             = 20 * time.Millisecond
)

type Log struct {
	Command interface{}
	Term    int
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	//mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	mu        deadlock.RWMutex
	killCh    chan struct{}

	lastActivityTime    time.Time
	electionTimeout     time.Duration
	applyToStateMachine chan ApplyMsg

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
	XTerm   int
	XIndex  int
	XLen    int
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	//start := time.Now() // Start timing
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()

	rf.persister.SaveRaftState(data)

	//duration := time.Since(start) // Measure elapsed time
	//Dprintf("Persist completed in... %d", duration)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Printf("Error decoding Raft state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.serverRole != LEADER {
		return -1, rf.currentTerm, false
	}

	// Append the command to the log
	rf.log = append(rf.log, Log{Term: rf.currentTerm, Command: command})
	index := len(rf.log) - 1 // This now correctly represents the entry's index starting from 1 for real commands

	DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[Term: %d]Got a leader, cmd processing... %d", rf.currentTerm, command)
	rf.persist()

	return index, rf.currentTerm, true
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.quorumSize = (len(peers) / 2) + 1
	rf.killCh = make(chan struct{})
	rf.currentTerm = 0
	rf.log = []Log{{
		Term:    0,
		Command: nil,
	}}
	rf.updateToFollowerRoleAndCatchUpNewTerm(0, false)

	// leader agreement inits
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyToStateMachine = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.lastActivityTime = time.Now()
	// start ticker goroutine to start elections

	go rf.ticker()
	go rf.applyLogEntriesPeriodically()

	DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Fully up")
	return rf
}

// RequestVote ---------------------- RPC CALLS  ---------------------- //

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogIndex, lastLogTerm := rf.lastLogTermIndex()

	DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[ELECTION_TIME_RESET], [Term: %d] AND TIME: %s, ELECTION TIMEOUT: %d", rf.currentTerm)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// If the current term is higher, reject the request vote
	if rf.currentTerm > args.Term {
		DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Current term is higher %d, rejecting the RequestVote for candidate %d and term: %d\n", rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	shouldPersist := false
	//if you have already voted in the current term, and an incoming RequestVote RPC has a higher term that you, you should first step down and adopt
	//their term (thereby resetting votedFor), and then handle the RPC, which will result in you granting the vote!
	if rf.currentTerm < args.Term {
		// if the request has higher term, it means you have a stale data. Update your term and change to follower.
		// You are allowed to vote in the new term.
		rf.updateToFollowerRoleAndCatchUpNewTerm(args.Term, false)
		reply.Term = args.Term
		shouldPersist = true
	}

	//// Step 4: Check if the candidate’s log is at least as up-to-date as this server’s log
	// Grant the vote
	votedForValid := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	candidateLogUpToDateCompareToFollower := !(lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex))

	if votedForValid && candidateLogUpToDateCompareToFollower {
		rf.votedFor = args.CandidateId

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		shouldPersist = true
		DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[@@@VOTE_GRANTED@@@] to candidateId %d, and term: %d, %d, lastLogIndex: %d, lastLogTerm:%d", rf.votedFor, args.Term, rf.log, lastLogIndex, lastLogTerm)
	}

	if shouldPersist {
		rf.persist()
		rf.resetElectionTimer()
	}

}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false

	// If outdated AppendEntries then reject the call.
	if rf.currentTerm > args.Term {
		DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Term is higher rejecting append entries new term: [%d], leader %d\n", rf.currentTerm, args.LeaderId)
		return
	}

	rf.resetElectionTimer()

	shouldPersist := false

	// Ensure that you follow the second rule in “Rules for Servers” before handling an incoming RPC. The second rule states:
	if rf.currentTerm < args.Term {
		reply.Term = args.Term
		rf.updateToFollowerRoleAndCatchUpNewTerm(args.Term, false)
		shouldPersist = true
		DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Got heartbeat from the new term: [%d], leader %d\n", rf.currentTerm, args.LeaderId)
	}

	logInconsistent := args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm

	if logInconsistent && shouldPersist {
		rf.persist()
	}

	if args.PrevLogIndex >= len(rf.log) {
		// Case 3: Follower log is too short
		reply.XLen = len(rf.log)
		reply.XTerm = -1
		reply.XIndex = -1
		//DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Follower log is too short: [%d], leader %d\n", reply.XLen, args.LeaderId)

		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Case 1: Leader does not have XTerm
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		// Find the first index of XTerm
		xIndex := args.PrevLogIndex
		for xIndex > 0 && rf.log[xIndex-1].Term == reply.XTerm {
			xIndex--
		}
		reply.XIndex = xIndex
		DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Leader doesn't have Xterm: [%d], Xindx %d\n", reply.XTerm, reply.XIndex)

		return
	}

	// Logs until previous Indexes are consistent, if it's here

	reply.Success = true

	if len(args.LogEntries) > 0 {
		// Find the position to start appending new entries
		indexToStartAppending := args.PrevLogIndex + 1

		// Delete any conflicting entries
		if indexToStartAppending < len(rf.log) {
			rf.log = rf.log[:indexToStartAppending]
		}

		// Append new entries
		beforeIndex := len(rf.log) - 1
		rf.log = append(rf.log, args.LogEntries...)
		shouldPersist = true
		DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Appending entry: %d, commmitIndx: %d", rf.log[beforeIndex:], rf.commitIndex)
	}

	if args.LeaderCommit > rf.commitIndex {
		// If it comes here, as a form of heartbeat or anything It's appending twice.
		beforeTerm := rf.commitIndex
		rf.commitIndex = minR(args.LeaderCommit, len(rf.log)-1)
		DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Updating commit Indx of follower from %d to %d", beforeTerm, rf.commitIndex)

	}

	if shouldPersist {
		rf.persist()
	}

	rf.resetElectionTimer()

}

// --------------------------- CMD/Logs Agreement  --------------------------------- //

func (rf *Raft) startAgreement() {
	startTime := time.Now()

	successCh := make(chan int, len(rf.peers))

	for i := range rf.peers {
		go func(server int) {
			for {
				select {
				case <-rf.killCh:
					return
				default:
					rf.mu.Lock()

					if rf.serverRole != LEADER {
						rf.mu.Unlock()
						continue
					}

					nextIndex := rf.nextIndex[server]

					if nextIndex > len(rf.log)-1 {
						rf.mu.Unlock()
						continue // No new entries to send
					}

					entries := rf.log[nextIndex:] // Entries to send
					prevLogIndex := rf.nextIndex[server] - 1
					prevLogTerm := 0
					if prevLogIndex > 0 {
						prevLogTerm = rf.log[prevLogIndex].Term
					}

					rf.mu.Unlock()

					matchIndex := rf.callAppendEntries(server, prevLogIndex, prevLogTerm, entries)

					if matchIndex != -1 {
						successCh <- matchIndex
					}

					time.Sleep(RPC_CALLS_SLEEP) // Randomized backoff
				}
			}
			// Problem inf loop
			////Dprintf("⏱ [Agreement Goroutine Duration] Node: %d for server %d took %v", rf.me, server, time.Since(loopStartTime))
		}(i)
	}

	go func() {
		for {
			select {
			case index := <-successCh:
				rf.mu.Lock()

				if index > rf.commitIndex {
					count := 0
					for _, matchIdx := range rf.matchIndex {
						if matchIdx >= index {
							count++
						}
					}
					if count >= rf.quorumSize && rf.log[index].Term == rf.currentTerm {
						rf.commitIndex = index
						DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[COMMIT--INDEX][commitIndx: %d] "+
							"commit index updated", rf.commitIndex)

					}
				}
				rf.mu.Unlock()
			case <-rf.killCh:
				return
			}
			time.Sleep(RPC_CALLS_SLEEP)
		}
	}()

	DPrintf("⏱ [Total Start Agreement Duration] Node: %d took %v", rf.me, time.Since(startTime))

}

func (rf *Raft) callAppendEntries(
	server int,
	prevLogIndex int,
	prevLogTerm int,
	entries []Log,
) int {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LogEntries:   entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply

	DPrintfId(rf.me, serverRoleToStr(rf.serverRole),
		"[REQUEST][AppendEntries][Term: %d][PrevLogIndex :%d][PrevLogTerm :%d][commitIndx: %d] "+
			"%d --> %d, entries to send: [%d], nextIndx: [%d]", rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, rf.commitIndex,
		rf.me, server, entries, rf.nextIndex)

	var ok = false
	if server == rf.me {
		ok = true
		reply.Success = true
	} else {
		ok = rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	}

	if !ok {
		return -1
	}

	rf.mu.Lock()

	if reply.Success {
		// Update nextIndex and matchIndex for success
		rf.nextIndex[server] = prevLogIndex + len(entries) + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1

		DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[SUCCESS][AppendEntries][Term: %d][PrevLogIndex :%d][PrevLogTerm :%d][commitIndx: %d] "+
			"%d --> %d, Update nextIndex %d matchIndx :%d and commitIndx %d",
			rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, rf.commitIndex, rf.me, server, rf.nextIndex, rf.matchIndex, rf.commitIndex)
		indx := rf.matchIndex[server]
		rf.mu.Unlock()
		return indx
	}

	if reply.Term > rf.currentTerm {
		rf.updateToFollowerRoleAndCatchUpNewTerm(reply.Term, true)
		rf.mu.Unlock()
		return -1
	}

	DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[FAILED][AppendEntries][Term: %d][PrevLogIndex :%d][PrevLogTerm :%d][commitIndx: %d] "+
		"%d --> %d, decrement nextIndex %d, [XTerm: %d, XIndx: %d, XLen: %d]", rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, rf.commitIndex, rf.me, server, rf.nextIndex, reply.XTerm, reply.XIndex, reply.XLen)

	// Handle the case where AppendEntries fails due to log inconsistency
	if reply.XTerm == -1 {
		// No specific term was found, fallback to decrement or set to XLen
		rf.nextIndex[server] = maxR(1, reply.XLen)
	} else {
		lastIndexForXTerm := rf.findLastIndexForTerm(reply.XTerm)
		if lastIndexForXTerm == -1 {
			// Leader does not have XTerm, set nextIndex to XIndex
			rf.nextIndex[server] = reply.XIndex
		} else {
			// Leader has XTerm, set nextIndex to the last index for XTerm + 1
			rf.nextIndex[server] = lastIndexForXTerm + 1
		}
	}
	rf.mu.Unlock()
	return -1
}

func (rf *Raft) applyLogEntriesPeriodically() {
	for {
		select {
		case <-rf.killCh:
			////Dprintf("🔚 [Apply Log Entries Periodically] Node: %d | Action: Stopped due to kill signal", rf.me)
			return // Exit the loop if a kill signal is received
		default:
			// Sleep first to provide initial delay and periodic pause
			time.Sleep(APPLYING_LOGS_AND_PERSIST_FREQ)

			// Now call the function to apply log entries with the copied values
			//startTime := time.Now()
			rf.applyLogEntriesToStateMachineLk()
			////Dprintf("⏱ [Apply Log Entries Periodically] Node: %d | Duration: %v", rf.me, time.Since(startTime))
		}
	}
}

func (rf *Raft) applyLogEntriesToStateMachineLk() {
	// Safely read the required shared variables
	rf.mu.Lock()
	lastApplied := rf.lastApplied
	commitIndex := rf.commitIndex
	logLen := len(rf.log)
	//role := serverRoleToStr(rf.serverRole)
	rf.mu.Unlock()

	if lastApplied < commitIndex && commitIndex < logLen {
		//Dprintf("📝 [Apply Log Entries] Node: %d | Role: %s | Range: %d - %d", rf.me, role, lastApplied+1, commitIndex)
		for i := lastApplied + 1; i <= commitIndex; i++ {
			rf.mu.Lock()
			if i < len(rf.log) {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i,
				}
				rf.lastApplied = i
				rf.mu.Unlock()
				rf.applyToStateMachine <- applyMsg
				////Dprintf("🔄 [Apply Log Entry] Node: %d | Entry Index: %d | Term: %d | Command: %v", rf.me, i, rf.log[i].Term, rf.log[i].Command)
			} else {
				rf.mu.Unlock()
			}
			time.Sleep(RPC_CALLS_SLEEP) // Simulate processing time per entry
		}
		//startPersistTime := time.Now()

		//Dprintf("💾 [Persist State] Node: %d | Duration: %v", rf.me, time.Since(startPersistTime))
	}
}
func (rf *Raft) findLastIndexForTerm(xTerm int) int {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == xTerm {
			return i
		}
	}
	return -1
}
func (rf *Raft) performLeadershipRituals() {
	lastLogIndex := len(rf.log) - 1 // Assuming log index starts at 1 and log slice includes a dummy entry at index 0

	if lastLogIndex == -1 {
		lastLogIndex = 0
	}

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}

	rf.persist()

}

// --------------------------- Leader election <--> Heartbeat --------------------------------- //

func (rf *Raft) sendHeartbeats() {
	for {
		select {
		case <-rf.killCh:
			// Exit the loop if the Raft instance is being stopped
			return
		default:
			// Perform heartbeat RPC call
			rf.mu.Lock()
			isLeader := rf.serverRole == LEADER
			rf.mu.Unlock()

			if isLeader {
				rf.rpcCallForHeartBeat()
			}
			// Sleep for the heartbeat interval duration
			time.Sleep(HEARTBEAT_INTERVAL)
		}
	}
}
func (rf *Raft) rpcCallForHeartBeat() {

	for indx, peer := range rf.peers {
		if rf.me == indx {
			continue
		} else {
			go func(index int, p *labrpc.ClientEnd) {

				rf.mu.Lock()
				lastLogIndex, lastLogTerm := rf.lastLogTermIndex()

				appendEntriesHeartBeatArgs := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					LogEntries:   []Log{},
					PrevLogIndex: lastLogIndex,
					PrevLogTerm:  lastLogTerm,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()

				var appendEntriesHeartBeatReply AppendEntriesReply
				ok := p.Call("Raft.AppendEntries", &appendEntriesHeartBeatArgs, &appendEntriesHeartBeatReply)

				if ok {
					rf.mu.Lock()
					if appendEntriesHeartBeatReply.Term > rf.currentTerm {
						rf.updateToFollowerRoleAndCatchUpNewTerm(appendEntriesHeartBeatReply.Term, true)
					}
					rf.mu.Unlock()
				}
			}(indx, peer)
		}
	}

}
func (rf *Raft) ticker() {
	for {
		rf.mu.Lock()
		lastActivity := rf.lastActivityTime
		electionTimeout := rf.electionTimeout
		//role := serverRoleToStr(rf.serverRole)
		rf.mu.Unlock()

		select {
		case <-rf.killCh:
			//Dprintf("🔚 [Ticker Stopped] | Node: %d | Role: %s | Action: Stopped due to kill signal", rf.me, role)
			return
		default:
			if time.Since(lastActivity) >= electionTimeout {
				go rf.startElection()
			}
			time.Sleep(TICKER_CALLS_SLEEP) // Adjusted to constant for simplicity
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	//initialRole := serverRoleToStr(rf.serverRole)

	if rf.serverRole == LEADER {
		////Dprintf("🛑 [Election Aborted] | Node: %d | Reason: Already Leader | Role: %s", rf.me, initialRole)
		rf.mu.Unlock()
		return
	}
	//Dprintf("🚀 [Start Election] | Node: %d | Initial Role: %s | Term: %d", rf.me, initialRole, rf.currentTerm)
	rf.resetElectionTimer()
	rf.updateToCandidateRole()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	voteCh := make(chan bool, len(rf.peers))

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
					rf.mu.Lock()
					if rf.serverRole == CANDIDATE {
						// Get the last log entry.
						lastLogTerm, lastLogIndex := 0, 0
						if len(rf.log) > 0 {
							lastLogIndex = len(rf.log) - 1
							lastLogTerm = rf.log[lastLogIndex].Term
						}

						requestVoteArgs := RequestVoteArgs{
							Term:         currentTerm,
							CandidateId:  rf.me,
							LastLogTerm:  lastLogTerm,
							LastLogIndex: lastLogIndex,
						}
						DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[RequestVote] %d ---> %d [Term: %d][LastLogIndx: %d][LastLogTerm: %d]", rf.me,
							peerIndex, rf.currentTerm, requestVoteArgs.LastLogIndex, requestVoteArgs.LastLogTerm)
						rf.mu.Unlock()

						var requestVoteReply RequestVoteReply
						ok := rf.peers[peerIndex].Call("Raft.RequestVote", &requestVoteArgs, &requestVoteReply)

						if ok {
							rf.mu.Lock()
							// Checking the term is same or not, that you requested vote for.
							if currentTerm == rf.currentTerm {
								if requestVoteReply.Term > rf.currentTerm {
									rf.updateToFollowerRoleAndCatchUpNewTerm(requestVoteReply.Term, true)
									voteCh <- false
								} else if requestVoteReply.VoteGranted && rf.currentTerm == requestVoteReply.Term {
									voteCh <- true
								} else {
									voteCh <- false
								}
							} else {
								voteCh <- false
							}
							rf.mu.Unlock()
						}
					} else {
						rf.mu.Unlock()
					}

					return
				}
			}
		}(indx)
	}

	grantedVotes := 1 // Start with self-vote
	for i := 1; i < len(rf.peers); i++ {
		if <-voteCh {
			grantedVotes++

			if grantedVotes >= rf.quorumSize {
				rf.mu.Lock()

				if rf.currentTerm == currentTerm && rf.serverRole == CANDIDATE {
					rf.updateToLeaderRole()
					DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Got enough vote %d [Term: %d]", grantedVotes, rf.currentTerm)
					//Dprintf("************* 👑👑👑👑👑👑 IAM THE LEADER 👑👑👑👑👑 %d**************", rf.me)
					rf.mu.Unlock()

					go rf.sendHeartbeats()
					go rf.startAgreement()

				} else {
					rf.mu.Unlock()
				}
				return
			}
		}
	}
}

// --------------------------- UTILS --------------------------------- //

func (rf *Raft) deleteConflictingEntriesFromIndx(fromIndex int) {
	if fromIndex < len(rf.log) {
		rf.log = rf.log[:fromIndex] // Slice the log up to, but not including, fromIndex
	}
}
func (rf *Raft) resetElectionTimer() {
	// Update the lastActivityTime to the current time
	rf.lastActivityTime = time.Now()
	// Randomize the next election timeout duration
	rf.electionTimeout = rf.randomizeElectionTimeout()
}
func (rf *Raft) updateToFollowerRoleAndCatchUpNewTerm(term int, persist bool) {
	DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "CATCH UP NEW TERM %s ---> FOLLOWER [Term: %d][NewTerm: %d] \n", serverRoleToStr(rf.serverRole), rf.currentTerm, term)
	rf.serverRole = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
	if persist {
		rf.persist()
	}
}

func (rf *Raft) randomizeElectionTimeout() time.Duration {
	return ElectionTimeoutMin + time.Duration(rand.Int63n(int64(ElectionTimeoutMax-ElectionTimeoutMin)))
}
func (rf *Raft) updateToCandidateRole() {
	rf.serverRole = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
}
func (rf *Raft) updateToLeaderRole() {
	rf.serverRole = LEADER
	rf.performLeadershipRituals()
}
func (rf *Raft) lastLogTermIndex() (int, int) {
	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term
	return lastIndex, lastTerm
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
func minR(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func maxR(a, b int) int {
	if a > b {
		return a
	}
	return b
}

//////////////////////////// USED BY TESTS /////////////////////////

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

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.serverRole == LEADER
}
