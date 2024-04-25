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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
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
	RPC_CALLS_SLEEP                = 50 * time.Millisecond
	TICKER_CALLS_SLEEP             = 20 * time.Millisecond
	RPC_CALLS_TIMEOUT              = 600 * time.Millisecond
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
	mu        sync.Mutex
	killCh    chan struct{}

	cancelElection      chan struct{}
	stepDownFromLeader  chan struct{}
	commandCh           chan Log
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
	localLog := Log{Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, localLog)
	index := len(rf.log) - 1 // This now correctly represents the entry's index starting from 1 for real commands

	DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[Term: %d]Got a leader, cmd processing... %d", rf.currentTerm, command)
	rf.persist()

	rf.commandCh <- localLog

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
	rf.cancelElection = make(chan struct{})
	rf.stepDownFromLeader = make(chan struct{})
	rf.commandCh = make(chan Log, 1000)

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
func (rf *Raft) processCommands() {
	var batch []Log
	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()

	for {
		select {
		case log := <-rf.commandCh:
			batch = append(batch, log)
			if len(batch) >= 1000 {
				rf.sendAppendEntriesBatch(batch)
				batch = nil
				timer.Reset(10 * time.Millisecond) // Reset timer after sending batch
			}
		case <-timer.C:
			if len(batch) > 0 {
				rf.sendAppendEntriesBatch(batch)
				batch = nil
			}
			timer.Reset(10 * time.Millisecond) // Reset timer for next interval
		case <-rf.stepDownFromLeader:
			return // Exit if no longer leader
		case <-rf.killCh:
			return // Exit on system shutdown
		}
	}
}

func (rf *Raft) sendAppendEntriesBatch(batch []Log) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.serverRole != LEADER {
		return
	}

	successCh := make(chan int, len(rf.peers)) // Create a channel to receive success indices

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			nextIndex := rf.nextIndex[server]
			prevLogIndex := nextIndex - 1
			prevLogTerm := 0
			if prevLogIndex > 0 && prevLogIndex < len(rf.log) {
				prevLogTerm = rf.log[prevLogIndex].Term
			}
			entries := rf.log[nextIndex:]
			matchIndex := rf.callAppendEntries(server, prevLogIndex, prevLogTerm, entries)
			if matchIndex != -1 {
				successCh <- matchIndex
			}
		}(i)
	}

	// Handle successes asynchronously
	go rf.handleSuccesses(successCh)
}

func (rf *Raft) handleSuccesses(successCh chan int) {
	for index := range successCh {
		rf.mu.Lock()
		if index > rf.commitIndex && rf.log[index].Term == rf.currentTerm {
			count := 1
			for _, matchIdx := range rf.matchIndex {
				if matchIdx >= index {
					count++
				}
			}
			if count >= rf.quorumSize {
				rf.commitIndex = index
				// Optionally trigger log commit to the state machine
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startAgreement() {
	startTime := time.Now()
	successCh := make(chan int, len(rf.peers)) // Channel for successful append entries
	quitCh := make(chan struct{})              // Channel to signal goroutines to stop

	// Start a goroutine for each peer
	for i := range rf.peers {
		go func(server int) {
			for {
				select {
				case <-rf.stepDownFromLeader:
					return // Exit if no longer leader
				case <-rf.killCh:
					return // Exit if the system is shutting down
				case <-quitCh:
					return // Controlled shutdown of goroutines
				default:
					rf.mu.Lock()
					if rf.serverRole != LEADER {
						rf.mu.Unlock()
						time.Sleep(RPC_CALLS_SLEEP) // Backoff if not leader
						continue
					}

					nextIndex := rf.nextIndex[server]
					if nextIndex >= len(rf.log) {
						rf.mu.Unlock()
						time.Sleep(RPC_CALLS_SLEEP) // No new entries to send
						continue
					}

					entries := rf.log[nextIndex:] // Entries to send
					prevLogIndex := nextIndex - 1
					prevLogTerm := 0
					if prevLogIndex > 0 {
						prevLogTerm = rf.log[prevLogIndex].Term
					}
					rf.mu.Unlock()

					matchIndex := rf.callAppendEntries(server, prevLogIndex, prevLogTerm, entries)
					if matchIndex != -1 {
						successCh <- matchIndex
					}
				}
			}
		}(i)
	}

	go func() {
		defer close(quitCh) // Ensure to signal all sender goroutines to stop after processing remaining successes.
		for {
			select {
			case <-rf.stepDownFromLeader:
				return
			case index := <-successCh:
				rf.handleSuccessAggreement(index)
			case <-rf.killCh:
				return
			}
		}
	}()

	DPrintf("⏱ [Total Start Agreement Duration] Node: %d took %v", rf.me, time.Since(startTime))

}

func (rf *Raft) handleSuccessAggreement(index int) {
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
			DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[COMMIT--INDEX][commitIndx: %d] commit index updated", rf.commitIndex)
		}
	}
	rf.mu.Unlock()
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
			return // Exit the loop if a kill signal is received
		default:
			time.Sleep(APPLYING_LOGS_AND_PERSIST_FREQ)
			rf.applyLogEntriesToStateMachine()
		}
	}
}

func (rf *Raft) applyLogEntriesToStateMachine() {
	var entriesToApply []Log
	rf.mu.Lock()
	lastApplied := rf.lastApplied
	commitIndex := rf.commitIndex
	if lastApplied < commitIndex && commitIndex < len(rf.log) {
		entriesToApply = rf.log[lastApplied+1 : commitIndex+1]
	}
	rf.mu.Unlock()

	for i, entry := range entriesToApply {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: lastApplied + 1 + i,
		}
		rf.applyToStateMachine <- applyMsg
	}

}

func (rf *Raft) applyLogEntriesToStateMachineLk() {
	// Safely read the required shared variables
	rf.mu.Lock()
	lastApplied := rf.lastApplied
	commitIndex := rf.commitIndex
	logLen := len(rf.log)
	rf.mu.Unlock()

	if lastApplied < commitIndex && commitIndex < logLen {
		for i := lastApplied + 1; i <= commitIndex; i++ {
			rf.mu.Lock()

			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
			rf.lastApplied = i
			rf.applyToStateMachine <- applyMsg
			rf.mu.Unlock()
			//Dprintf("🔄 [Apply Log Entry] Node: %d | Entry Index: %d | Term: %d | Command: %v", rf.me, i, rf.log[i].Term, rf.log[i].Command)

			time.Sleep(RPC_CALLS_SLEEP) // Simulate processing time per entry
		}
	}
}

// --------------------------- Heartbeat --------------------------------- //

func (rf *Raft) sendHeartbeats() {
	for {
		select {
		case <-rf.killCh:
			// Exit the loop if the Raft instance is being stopped
			return
		case <-rf.stepDownFromLeader:
			// Stop sending heartbeats if not leader anymore
			return
		default:
			// Perform heartbeat RPC call
			rf.rpcCallForHeartBeat()

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
					if appendEntriesHeartBeatReply.Term > appendEntriesHeartBeatArgs.Term {
						rf.mu.Lock()
						rf.updateToFollowerRoleAndCatchUpNewTerm(appendEntriesHeartBeatReply.Term, true)
						rf.mu.Unlock()
					}
				}
			}(indx, peer)
		}
	}

}

// --------------------------- Leader election  --------------------------------- //

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
				close(rf.cancelElection)                // Close the channel to signal cancellation
				go rf.startElection()                   // Start a new election
				rf.cancelElection = make(chan struct{}) // Reinitialize the cancellation channel
			}
			time.Sleep(TICKER_CALLS_SLEEP) // Adjusted to constant for simplicity
		}
	}
}
func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.serverRole == LEADER {
		rf.mu.Unlock()
		return
	}
	DPrintf("🚀 [Start Election] | Node: %d | Term: %d", rf.me, rf.currentTerm)
	rf.resetElectionTimer()
	rf.updateToCandidateRole()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	voteCh := make(chan bool, len(rf.peers)-1)
	var wg sync.WaitGroup

	for indx := range rf.peers {
		if rf.me == indx {
			continue
		}
		wg.Add(1)
		go rf.sendRequestVote(indx, currentTerm, voteCh, &wg)
	}

	// Wait for all votes to be cast and then close the channel
	go func() {
		wg.Wait()
		close(voteCh)
	}()

	go rf.countVotes(voteCh, currentTerm)
}
func (rf *Raft) sendRequestVote(peerIndex int, term int, voteCh chan<- bool, wg *sync.WaitGroup) {
	defer wg.Done()

	rf.mu.Lock()
	lastLogTerm, lastLogIndex := 0, 0
	if len(rf.log) > 0 {
		lastLogIndex = len(rf.log) - 1
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	requestVoteArgs := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogTerm:  lastLogTerm,
		LastLogIndex: lastLogIndex,
	}
	rf.mu.Unlock()

	var requestVoteReply RequestVoteReply
	doneChan := make(chan bool, 1)

	go func() {
		ok := rf.peers[peerIndex].Call("Raft.RequestVote", &requestVoteArgs, &requestVoteReply)
		doneChan <- ok
	}()

	select {
	case ok := <-doneChan:
		if ok {
			rf.mu.Lock()
			if term == rf.currentTerm {
				if requestVoteReply.Term > rf.currentTerm {
					rf.updateToFollowerRoleAndCatchUpNewTerm(requestVoteReply.Term, true)
				} else if requestVoteReply.VoteGranted && rf.currentTerm == requestVoteReply.Term {
					voteCh <- true
				}
			}
			rf.mu.Unlock()
		}
	case <-time.After(RPC_CALLS_TIMEOUT):
		return
	case <-rf.cancelElection:
		return
	}
}
func (rf *Raft) countVotes(voteCh <-chan bool, currentTerm int) {
	grantedVotes := 1 // Start with self-vote
	for vote := range voteCh {
		if vote {
			grantedVotes++
			DPrintf("[Vote Granted] | Node: %d | Current Votes: %d", rf.me, grantedVotes)
		}
		if grantedVotes >= rf.quorumSize {
			rf.mu.Lock()
			if rf.currentTerm == currentTerm && rf.serverRole == CANDIDATE {
				rf.updateToLeaderRole()
				DPrintf("👑 [New Leader Elected] | Node: %d | Term: %d", rf.me, rf.currentTerm)
				go rf.sendHeartbeats()
				go rf.startAgreement()
			}
			rf.mu.Unlock()
			break
		}
	}
}

// --------------------------- Utils --------------------------------- //

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
}
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
	wasLeader := rf.serverRole == LEADER
	rf.serverRole = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
	if wasLeader {
		close(rf.stepDownFromLeader) // This will signal to stop the heartbeats
	}
	if persist {
		rf.persist()
	}
}

func (rf *Raft) randomizeElectionTimeout() time.Duration {
	return ElectionTimeoutMin + time.Duration(rand.Int63n(int64(ElectionTimeoutMax-ElectionTimeoutMin)))
}
func (rf *Raft) updateToCandidateRole() {
	wasLeader := rf.serverRole == LEADER
	rf.serverRole = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
	if wasLeader {
		close(rf.stepDownFromLeader) // This will signal to stop the heartbeats
	}
	rf.persist()
}
func (rf *Raft) updateToLeaderRole() {
	rf.serverRole = LEADER
	rf.stepDownFromLeader = make(chan struct{})
	rf.performLeadershipRituals()
	rf.persist()
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
