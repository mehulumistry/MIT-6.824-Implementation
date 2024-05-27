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
	"fmt"
	"github.com/mehulumistry/MIT-6.824-Implementation/pkg/labgob"
	"github.com/mehulumistry/MIT-6.824-Implementation/pkg/labrpc"
	"log"
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
	FOLLOWER ServerRole = iota
	CANDIDATE
	LEADER
)

const (
	ElectionTimeoutMin             = 400 * time.Millisecond
	ElectionTimeoutMax             = 800 * time.Millisecond
	APPLYING_LOGS_AND_PERSIST_FREQ = 50 * time.Millisecond
	APPEND_ENTRIES_CALLS_FREQ      = 100 * time.Millisecond
)

type Log struct {
	Index   int
	Command interface{}
	Term    int
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	mu        sync.Mutex
	killCh    chan struct{}

	electionStartTime time.Time

	cancelElection chan struct{}
	stopLeader     chan struct{}

	electionTimer       *time.Timer
	applyToStateMachine chan ApplyMsg

	heartbeatTimer *time.Timer
	resetTimerCh   chan struct{}
	// static throughout, it'll only change if you are implementing membership change.
	quorumSize int

	// dynamic vars, don't forget to reset
	serverRole ServerRole

	// persisted state on all servers
	currentTerm int
	votedFor    int // candidateId that receive vote on current term
	log         []Log

	lastSnapshotIndex int
	lastSnapshotTerm  int
	commitIndex       int
	lastApplied       int

	leaderId   int
	nextIndex  []int
	matchIndex []int

	notifyApplyCh chan struct{}
	notifySnapCh  chan ApplyMsg

	lg *LoggingUtils // Vector clock for the entry (optional)

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
	Term         int // currentTerm, for candidate to update itself.
	Success      bool
	XTerm        int
	XIndex       int
	StaleRequest bool
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

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderID          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	SnapShot          []byte // raw byte data of the snapshot
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

// ---------------------- persist functions ----------------------- //
func (rf *Raft) getRaftStatePersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	return w.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	defer rf.deferLoggingWithLog("[WRITE_PERSIST_DONE]", "", rf.me)

	data := rf.getRaftStatePersistData()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	defer rf.deferLoggingWithLog("[READ_PERSIST_DONE]", "", rf.me)

	if data == nil || len(data) < 1 { // Check if there's any state data to read
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int
	var votedFor int
	var logs []Log
	var lastSnapshotIndex, lastSnapshotTerm int

	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastSnapshotIndex) != nil ||
		d.Decode(&lastSnapshotTerm) != nil {
		log.Fatal("raft read persist error")
	} else {
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = logs
		rf.lastSnapshotIndex = lastSnapshotIndex
		rf.lastSnapshotTerm = lastSnapshotTerm
	}
}

// --------------------------- Utils --------------------------------- //

// truncateLog removes log entries up to and including the lastIncludedIndex
// and ensures the first log entry starts right after lastIncludedIndex with the correct term.
// It also ensures that the new log at index 0 starts with the lastIncludedIndex and lastIncludedTerm.
// CondInstallSnapshot A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	defer rf.deferLogging("[CondInstallSnapshot]", "", rf.me, time.Now())

	// Check if the snapshot is more recent than this server's current state

	// Install the snapshot:
	// 1. Update the state machine and apply the snapshot
	// 2. Adjust the Raft state (commitIndex, lastApplied, etc.)
	// 3. Clear and update the log appropriately

	rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole),
		"[SNAPSHOT][INSTALL_SNAPSHOTS][Term: %d][commitIndexFromTester: %d, commitIndex: %d][lastApplied: %d, lastSnapshotIndex: %d], [newLog: %d]", rf.currentTerm, lastIncludedIndex,
		rf.commitIndex, rf.lastApplied, rf.lastSnapshotIndex, rf.log)

	rf.log = truncateLog(lastIncludedIndex, rf.log)
	rf.lastSnapshotIndex = lastIncludedIndex
	rf.lastSnapshotTerm = lastIncludedTerm
	rf.lastApplied = maxR(rf.lastSnapshotIndex, rf.lastApplied)

	rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole),
		"[SNAPSHOT][INSTALLED][Term: %d][commitIndexFromTester: %d, commitIndex: %d][lastApplied: %d, lastSnapshotIndex: %d], [newLog: %d]", rf.currentTerm, lastIncludedIndex,
		rf.commitIndex, rf.lastApplied, rf.lastSnapshotIndex, rf.log)

	// Persist snapshot state along with the truncated log
	rf.persister.SaveStateAndSnapshot(rf.getRaftStatePersistData(), snapshot)

	return true
}

func (rf *Raft) handleAppendEntriesSuccessAgreement(index int) {

	indx := rf.getIndexAddOffset(index, false)

	if index > rf.commitIndex {
		count := 1
		for _, matchIdx := range rf.matchIndex {
			if matchIdx >= index {
				count++
			}
		}
		if count >= rf.quorumSize && rf.log[indx].Term == rf.currentTerm {
			rf.commitIndex = index
			rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[COMMIT--INDEX][commitIndx: %d] commit index updated", rf.commitIndex)

			rf.notifyApplyCh <- struct{}{}
			rf.persist()

		}
	}

}
func (rf *Raft) applyLogEntriesToStateMachine() {

	var entriesToApply []Log
	rf.mu.Lock()
	lastApplied := rf.lastApplied
	commitIndex := rf.commitIndex
	if lastApplied < commitIndex && commitIndex < rf.getLogLengthWithSnapShotOffset() {
		entriesToApply = rf.log[rf.getIndexAddOffset(lastApplied+1, false):rf.getIndexAddOffset(commitIndex+1, false)]
	}
	if len(entriesToApply) > 0 {
		rf.lastApplied = entriesToApply[len(entriesToApply)-1].Index // Update lastApplied for each applied entry
		rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole),
			"[FINISHED][APPLY_LOGS_TO_CHANNEL][Term: %d][commitIndx: %d][lastApplied: %d]---", rf.currentTerm, rf.commitIndex, rf.lastApplied)
	}

	rf.mu.Unlock()

	for _, entry := range entriesToApply {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
		}
		rf.applyToStateMachine <- applyMsg
	}

}
func (rf *Raft) findLastIndexForTerm(xTerm int) int {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == xTerm {
			//return i
			return rf.log[i].Index
		}
	}
	return -1
}
func (rf *Raft) performLeadershipRituals() {
	lastLogIndex := rf.lastSnapshotIndex + len(rf.log) - 1

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
func (rf *Raft) getIndexAddOffset(indx int, offset bool) int {
	if offset {
		return rf.log[indx-rf.lastSnapshotIndex].Index
	}
	return indx - rf.lastSnapshotIndex
}
func (rf *Raft) getLogLengthWithSnapShotOffset() int {
	return rf.log[len(rf.log)-1].Index + 1
}
func (rf *Raft) initFollowerRole() {
	rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "INIT NEW TERM %s ---> FOLLOWER [Term: %d][NewTerm: %d] \n", serverRoleToStr(rf.serverRole), rf.currentTerm, 0)
	rf.serverRole = FOLLOWER
	rf.votedFor = -1
}
func (rf *Raft) updateToFollowerRoleAndCatchUpNewTerm(term int) {
	wasLeader := rf.serverRole == LEADER
	wasCandidate := rf.serverRole == CANDIDATE
	rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "CATCH UP NEW TERM %s ---> FOLLOWER [Term: %d][NewTerm: %d] \n", serverRoleToStr(rf.serverRole), rf.currentTerm, term)

	rf.serverRole = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
	if wasLeader {
		close(rf.stopLeader)
	}
	if wasCandidate {
		close(rf.cancelElection)
	}
	rf.persist()
	rf.resetElectionTimer()
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.heartbeatTimer.Stop()
	rf.heartbeatTimer.Reset(APPEND_ENTRIES_CALLS_FREQ)
}
func (rf *Raft) resetElectionTimer() {
	duration := rf.randomizeElectionTimeout()
	rf.electionTimer.Reset(duration)
	rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[TIMER_RESET][NEW_TIME :%s][TIME_LEFT_BEFORE :%s]", duration.String(), time.Since(rf.electionStartTime))
	rf.electionStartTime = time.Now().Add(duration)
}
func (rf *Raft) randomizeElectionTimeout() time.Duration {
	timeout := ElectionTimeoutMin + time.Duration(rand.Intn(int(ElectionTimeoutMax-ElectionTimeoutMin)))
	return timeout
}
func (rf *Raft) deferLogging(action string, serviceName string, forServer int, currentTime time.Time) {
	rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole),
		"[FINISHED][%s][service: %s][Term: %d][commitIndx: %d][TIME_LEFT: %s][ProcessingTime: %s]  serverId: [%d], nextIndx: [%d][snapShotIndex: %d, snapshotTerm: %d]",
		action, serviceName, rf.currentTerm, rf.commitIndex,
		time.Since(rf.electionStartTime).String(), time.Since(currentTime), forServer, rf.nextIndex,
		rf.lastSnapshotIndex, rf.lastSnapshotTerm)
}
func (rf *Raft) deferLoggingWithLog(action string, serviceName string, forServer int) {
	rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole),
		"[FINISHED][%s][service: %s][Term: %d][commitIndx: %d][TIME_LEFT: %s]  serverId: [%d], nextIndx: [%d]", action, serviceName, rf.currentTerm, rf.commitIndex,
		time.Since(rf.electionStartTime).String(), forServer, rf.nextIndex)
}
func (rf *Raft) updateToCandidateRole(requestId string) {
	defer rf.deferLogging("updateToCandidateRole", "", rf.me, time.Now())
	wasLeader := rf.serverRole == LEADER
	wasCandidate := rf.serverRole == CANDIDATE
	rf.serverRole = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
	if wasLeader {
		close(rf.stopLeader) // This will signal to stop the heartbeats
	}
	if wasCandidate {
		close(rf.cancelElection)
	}
	if !wasCandidate {
		rf.persist()
	}
	rf.leaderId = -1
	rf.cancelElection = make(chan struct{})
	rf.resetElectionTimer()
}
func (rf *Raft) updateToLeaderRole() {
	rf.serverRole = LEADER
	rf.stopLeader = make(chan struct{})
	rf.leaderId = rf.me
	rf.performLeadershipRituals()
	rf.persist()
}
func (rf *Raft) lastLogIndexTerm() (int, int) {
	lastIndex := rf.log[len(rf.log)-1].Index
	lastTerm := rf.log[rf.getIndexAddOffset(lastIndex, false)].Term
	return lastIndex, lastTerm
}

// -----------------------------

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
func truncateLog(lastIncludedIndex int, log []Log) []Log {
	var newLog []Log
	// Iterate over the existing log entries
	for _, entry := range log {
		if entry.Index >= lastIncludedIndex {
			newLog = append(newLog, entry)
		}
	}
	return newLog
}
func generateRequestID() string {
	return fmt.Sprintf("%d", time.Now().Unix())
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
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.serverRole == LEADER
}

func (rf *Raft) StartIfLeader(command interface{}) (int, int, int) {
	rf.mu.Lock()
	isLeader := rf.serverRole == LEADER
	currentTerm := rf.currentTerm

	if !isLeader {
		rf.mu.Unlock()
		return -1, rf.currentTerm, rf.leaderId
	}

	rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[Term: %d]Got a leader, cmd processing... %+v", rf.currentTerm, command)

	// Append the command to the log
	index := rf.getLogLengthWithSnapShotOffset() // This now correctly represents the entry's index starting from 1 for real commands
	rf.log = append(rf.log, Log{Term: rf.currentTerm, Command: command, Index: index})

	rf.mu.Unlock()

	rf.resetHeartbeatTimer()
	rf.sendAppendEntriesToPeer()

	return index, currentTerm, rf.me
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index, term, leader := rf.StartIfLeader(command)
	return index, term, leader == rf.me
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

	rf.lg = &LoggingUtils{
		//logger: peers[rf.me].GetLogger(),
		debug: true,
	}

	rf.mu.Lock()
	rf.quorumSize = (len(peers) / 2) + 1
	rf.killCh = make(chan struct{})
	rf.currentTerm = 0
	rf.log = []Log{{
		Term:    0,
		Command: nil,
	}}

	rf.leaderId = -1

	rf.notifyApplyCh = make(chan struct{}, 10000)
	rf.notifySnapCh = make(chan ApplyMsg, 10)

	rf.initFollowerRole()
	rf.applyToStateMachine = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.electionTimer = time.NewTimer(rf.randomizeElectionTimeout())

	rf.commitIndex = rf.lastSnapshotIndex
	rf.lastApplied = rf.lastSnapshotIndex

	rf.mu.Unlock()
	go rf.ticker()
	go rf.applyLogEntries()

	rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Fully up")
	return rf
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(commitIndexFromTester int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.deferLogging("[Snapshot]", "", rf.me, time.Now())

	// Check if snapshotting is needed
	if commitIndexFromTester <= rf.lastSnapshotIndex || commitIndexFromTester > rf.commitIndex {
		rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole),
			"[FINISHED][NOTHING_TO_SNAPSHOT_FOR][Term: %d][commitIndexFromTester: %d, commitIndex: %d][lastApplied: %d, lastSnapshotIndex: %d]", rf.currentTerm, commitIndexFromTester,
			rf.commitIndex, rf.lastApplied, rf.lastSnapshotIndex)
		return
	}
	// Find the last term at the snapshot index

	term := rf.log[commitIndexFromTester-rf.lastSnapshotIndex].Term

	rf.CondInstallSnapshot(term, commitIndexFromTester, snapshot)

}

// --------------------------- Main Functions  --------------------------------- //

func (rf *Raft) ticker() {
	// Initialize the timer with a randomized election timeout
	defer rf.deferLogging("[ELECTION TICKER STOPPED, PLEASE DEBUG MEEEEEE]", serverRoleToStr(rf.serverRole)+"!!!!!!!!!!!!!!!!!!!!!!!DANGERRRRR !!!!!!!!!!!!!!!!!!!!!!!", rf.me, time.Now())
	for {
		time.Sleep(20 * time.Millisecond)

		select {
		case <-rf.killCh:
			// Handle the stop signal, clean up resources
			if !rf.electionTimer.Stop() {
				<-rf.electionTimer.C
			}
			return

		case <-rf.electionTimer.C:
			// Election timeout occurred, start an election
			go rf.startElection() // Start a new election
		}
	}
}
func (rf *Raft) startElection() {
	defer rf.deferLogging("REQUEST_VOTES", "StartElection", rf.me, time.Now())

	rf.mu.Lock()
	rf.lg.DPrintf("🚀🚀🚀🚀🚀🚀🚀 [Start Election] | Node: %d | Term: %d | timeNow :%s", rf.me, rf.currentTerm+1, time.Now().String())

	rf.updateToCandidateRole(fmt.Sprintf("Term: %d", rf.currentTerm+1))
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	voteCh := make(chan bool, len(rf.peers)-1)

	var wg sync.WaitGroup

	for indx := range rf.peers {
		if rf.me != indx {
			wg.Add(1)
			go rf.sendRequestVote(indx, currentTerm, voteCh, &wg)
		}
	}

	// Wait for all votes to be cast and then close the channel
	go func() {
		wg.Wait()
		close(voteCh)
		rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[CLOSED_VOTING_CHANNEL][REQUEST_VOTE][Term: %d]", rf.currentTerm)
	}()

	grantedVotes := 1

	go func() {
		defer rf.deferLogging("COUNT_VOTES", "CountVotes ACKs", rf.me, time.Now())

		for {
			select {
			case vote := <-voteCh:
				if vote {
					grantedVotes++
					rf.lg.DPrintf("[Vote Granted] | Node: %d | Current Votes: %d, quorumSize: %d", rf.me, grantedVotes, rf.quorumSize)
				}
				if grantedVotes >= rf.quorumSize {
					rf.mu.Lock()
					if rf.currentTerm == currentTerm && rf.serverRole == CANDIDATE {

						rf.heartbeatTimer = time.NewTimer(APPEND_ENTRIES_CALLS_FREQ)
						rf.updateToLeaderRole()
						rf.mu.Unlock()
						log.Printf("👑 [New Leader Elected] | Node: %d | Term: %d", rf.me, rf.currentTerm)

						go rf.heartBeatTicker()
					} else {
						rf.mu.Unlock()
					}
				}
			case <-rf.killCh:
				return
			case <-rf.cancelElection:
				return
			}
		}
	}()
}

func (rf *Raft) sendRequestVote(peerIndex int, term int, voteCh chan<- bool, wg *sync.WaitGroup) {
	defer wg.Done()
	defer rf.deferLogging("SendRequestVoteEachServer", "RPC PROCESS REQUEST", peerIndex, time.Now())

	rf.mu.Lock()
	lastLogIndex, lastLogTerm := rf.lastLogIndexTerm()
	requestVoteArgs := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogTerm:  lastLogTerm,
		LastLogIndex: lastLogIndex,
	}
	isCandidate := rf.serverRole == CANDIDATE

	rf.mu.Unlock()

	var requestVoteReply RequestVoteReply

	if isCandidate {
		rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole),
			"[REQUEST][REQUEST_VOTE][Term: %d][PrevLogIndex :%d][PrevLogTerm :%d][commitIndx: %d] "+
				"%d --> %d, nextIndx: [%d]", rf.currentTerm, requestVoteArgs.LastLogIndex, requestVoteArgs.LastLogTerm, rf.commitIndex,
			rf.me, peerIndex, rf.nextIndex)

		ok := rf.peers[peerIndex].CallWithVClock(rf.me, "Raft.RequestVote", &requestVoteArgs, &requestVoteReply)

		if !ok {
			rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole),
				"[REQUEST][REQUEST_VOTE_FAILURE][Term: %d][PrevLogIndex :%d][PrevLogTerm :%d][commitIndx: %d] "+
					"%d --> %d, nextIndx: [%d]", rf.currentTerm, requestVoteArgs.LastLogIndex, requestVoteArgs.LastLogTerm, rf.commitIndex,
				rf.me, peerIndex, rf.nextIndex)
			return
		}

		rf.mu.Lock()

		if requestVoteReply.Term > rf.currentTerm {

			rf.updateToFollowerRoleAndCatchUpNewTerm(requestVoteReply.Term)

			rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[REQUEST_VOTE_ACK][STEPPING_DOWN_TO_FOLLOWER][SUCCESS][Term: %d] from serverId: [%d]  "+
				"", rf.currentTerm)
		} else if requestVoteReply.VoteGranted && rf.currentTerm == requestVoteReply.Term && rf.serverRole == CANDIDATE && rf.currentTerm == requestVoteArgs.Term {
			voteCh <- true
		}

		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) sendAppendEntriesToPeer() {
	for server := range rf.peers {
		if rf.me != server {
			go rf.callAppendEntries(server)
		}
	}
}

func (rf *Raft) heartBeatTicker() {
	for {
		select {
		case <-rf.stopLeader:
			return
		case <-rf.killCh:
			return
		case <-rf.heartbeatTimer.C:
			rf.resetHeartbeatTimer()
			rf.sendAppendEntriesToPeer()
		}
	}
}

func (rf *Raft) callAppendEntries(server int) {

	rf.mu.Lock()
	if rf.serverRole != LEADER {
		rf.mu.Unlock()
		return
	}
	nextIndexBeforeRPC := rf.nextIndex[server]
	term := rf.currentTerm
	//prevLogIndex := nextIndexBeforeRPC - 1

	if nextIndexBeforeRPC <= rf.lastSnapshotIndex {
		nextIndexBeforeRPC = rf.lastSnapshotIndex + 1
	}

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: nextIndexBeforeRPC - 1,
		PrevLogTerm:  rf.log[nextIndexBeforeRPC-rf.lastSnapshotIndex-1].Term,
		LogEntries:   rf.log[nextIndexBeforeRPC-rf.lastSnapshotIndex:],
		LeaderCommit: rf.commitIndex,
	}

	rf.mu.Unlock()

	var reply AppendEntriesReply

	rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole),
		"[REQUEST][%s][Term: %d][PrevLogIndex :%d][PrevLogTerm :%d][commitIndx: %d] "+
			"%d --> %d, entries to send: [%+v], nextIndx: [%d]", "APPEND_ENTRIES", rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, rf.commitIndex, rf.me, server, args.LogEntries, rf.nextIndex)

	ok := rf.peers[server].CallWithVClock(rf.me, "Raft.AppendEntries", &args, &reply)
	if !ok {
		rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[FAILURE-ACK][%s][Term: %d][PrevLogIndex :%d][PrevLogTerm :%d][commitIndx: %d] "+
			"%d --> %d, Update nextIndex %d matchIndx :%d and commitIndx %d",
			"APPEND_ENTRIES", rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, rf.commitIndex, rf.me, server, rf.nextIndex, rf.matchIndex, rf.commitIndex)
		return
	}

	if reply.StaleRequest {
		rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[STALE_REQUEST][%s][Term: %d][PrevLogIndex :%d][PrevLogTerm :%d][commitIndx: %d] "+
			"%d --> %d, Update nextIndex %d matchIndx :%d and commitIndx %d",
			"APPEND_ENTRIES", rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, rf.commitIndex, rf.me, server, rf.nextIndex, rf.matchIndex, rf.commitIndex)
		return
	}

	rf.mu.Lock()

	if reply.Term > rf.currentTerm {
		rf.updateToFollowerRoleAndCatchUpNewTerm(reply.Term)
		rf.mu.Unlock()
		return
	}

	if rf.serverRole != LEADER || rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return
	}

	rf.resetElectionTimer()

	if nextIndexBeforeRPC <= rf.lastSnapshotIndex {
		rf.sendSnapshot(server)
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[SUCCESS][%s][Term: %d][PrevLogIndex :%d][PrevLogTerm :%d][commitIndx: %d] "+
			"%d --> %d, Update nextIndex %d matchIndx :%d and commitIndx %d",
			"APPEND_ENTRIES", rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, rf.commitIndex, rf.me, server, rf.nextIndex, rf.matchIndex, rf.commitIndex)

		rf.nextIndex[server] = nextIndexBeforeRPC + len(args.LogEntries)
		rf.matchIndex[server] = args.PrevLogIndex + len(args.LogEntries)

		indx := rf.matchIndex[server]
		rf.handleAppendEntriesSuccessAgreement(indx)

		rf.mu.Unlock()

		return
	} else {
		if rf.lastSnapshotIndex > reply.XIndex {
			rf.nextIndex[server] = rf.lastSnapshotIndex + 1
			rf.sendSnapshot(server)
		} else {
			if reply.XIndex-rf.lastSnapshotIndex > len(rf.log)-1 {
				rf.nextIndex[server] = rf.lastSnapshotIndex + 1
			} else {
				if rf.log[reply.XIndex-rf.lastSnapshotIndex].Term == reply.XTerm {
					rf.nextIndex[server] = reply.XIndex + 1
				} else {
					rf.nextIndex[server] = reply.XIndex
				}
			}
		}
	}

	rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[MISMATCH][FAILED][%s][Term: %d][PrevLogIndex :%d][PrevLogTerm :%d][commitIndx: %d] "+
		"%d --> %d, decrement nextIndex %d, [XTerm: %d, XIndx: %d]", "APPEND_ENTRIES", rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, rf.commitIndex, rf.me, server, rf.nextIndex, reply.XTerm, reply.XIndex)
	rf.mu.Unlock()
	return

}
func (rf *Raft) sendSnapshot(server int) {

	// Prepare the snapshot data to be sent
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		SnapShot:          rf.persister.ReadSnapshot(),
	}

	rf.mu.Unlock()
	// Create a reply object
	reply := InstallSnapshotReply{}

	ok := rf.peers[server].CallWithVClock(rf.me, "Raft.InstallSnapshot", &args, &reply)

	rf.mu.Lock()

	if !ok {
		// Log failure or handle retries
		rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Failed to send snapshot to server %d", server)
		return // Indicates failure to send the snapshot
	}

	// Check if the follower's term is greater than the leader's term
	if reply.Term > rf.currentTerm {
		rf.updateToFollowerRoleAndCatchUpNewTerm(reply.Term)
		return // Indicates that the current server is no longer a leader
	}

	if rf.currentTerm != args.Term || rf.serverRole != LEADER {
		return
	}

	if args.LastIncludedIndex > rf.matchIndex[server] {
		rf.matchIndex[server] = rf.getIndexAddOffset(args.LastIncludedIndex+1, false)
	}

	if args.LastIncludedIndex+1 > rf.nextIndex[server] {
		rf.nextIndex[server] = args.LastIncludedIndex + 1
	}

	rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Snapshot successfully sent to server %d, nextIndex set to %d", server, rf.nextIndex[server])

	return
}
func (rf *Raft) applyLogEntries() {
	for {
		select {
		case <-rf.killCh:
			return // Exit the loop if a kill signal is received
		case <-rf.notifyApplyCh:
			rf.applyLogEntriesToStateMachine()
		}
	}
}

// RequestVote ---------------------- RPC CALLS  ---------------------- //

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	defer rf.deferLogging("RequestVote", fmt.Sprintf("RPC PROCESS REQUEST]"), rf.me, time.Now())
	rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[PROCESSING_REQUEST][RequestVote] from the candidateId %d, leader term: %d \n", args.CandidateId, args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// If the current term is higher, reject the request vote
	if rf.currentTerm > args.Term {
		rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Current term is higher %d, rejecting the RequestVote for candidate %d and term: %d\n", rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	//if you have already voted in the current term, and an incoming RequestVote RPC has a higher term that you, you should first step down and adopt
	//their term (thereby resetting votedFor), and then handle the RPC, which will result in you granting the vote!
	if rf.currentTerm < args.Term {
		// if the request has higher term, it means you have a stale data. Update your term and change to follower.
		// You are allowed to vote in the new term.
		rf.updateToFollowerRoleAndCatchUpNewTerm(args.Term)
		reply.Term = args.Term
	}

	//// Step 4: Check if the candidate’s log is at least as up-to-date as this server’s log
	// Grant the vote
	lastLogIndex, lastLogTerm := rf.lastLogIndexTerm()
	votedForValid := rf.votedFor == -1 || rf.votedFor == args.CandidateId

	candidateLogUpToDateCompareToFollower := lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex)

	if votedForValid && candidateLogUpToDateCompareToFollower {
		rf.votedFor = args.CandidateId

		reply.VoteGranted = true
		rf.persist()
		rf.resetElectionTimer()
		rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[@@@VOTE_GRANTED@@@] voted for candidateId %d, and term: %d, "+
			"lastLogIndex: %d, lastLogTerm:%d, args: [indx: %d, term: %d]", rf.votedFor, args.Term, lastLogIndex, lastLogTerm, args.LastLogIndex, args.LastLogTerm)
	} else {
		rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[REJECTING_VOTE] to candidateId %d, and term: %d, lastLogIndex: %d, lastLogTerm:%d, args: [indx: %d, term: %d] upToDate: %s, votedForValid :%s, votedFor: %d",
			args.CandidateId, args.Term, lastLogIndex, lastLogTerm, args.LastLogIndex, args.LastLogTerm, candidateLogUpToDateCompareToFollower, votedForValid, rf.votedFor)
	}

}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.deferLogging("RPCRequestProcessed", "AppendEntries", rf.me, time.Now())
	rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[PROCESSING_REQUEST][AppendEntries]from the leader %d, leader term: %d \n", args.LeaderId, args.Term)

	lastLogIndex, lastLogTerm := rf.lastLogIndexTerm()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.XTerm = lastLogTerm
	reply.XIndex = lastLogIndex
	reply.StaleRequest = false

	// If outdated AppendEntries then reject the call.
	if rf.currentTerm > args.Term {
		rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Term is higher rejecting append entries new term: [%d], leader %d\n", rf.currentTerm, args.LeaderId)
		return
	}

	rf.resetElectionTimer()

	// Ensure that you follow the second rule in “Rules for Servers” before handling an incoming RPC. The second rule states:
	if rf.currentTerm < args.Term || (rf.currentTerm == args.Term && rf.serverRole == CANDIDATE) {
		reply.Term = args.Term
		rf.updateToFollowerRoleAndCatchUpNewTerm(args.Term)
		rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Got heartbeat from the new term: [%d], leader %d\n", rf.currentTerm, args.LeaderId)
	}

	// Logs until previous Indexes are consistent, if it's here
	rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Got something from leader "+
		"WithOffset: [logLen: %d, argsPrevIndex: %d, argsPrevTerm: %d, lastSnapShotIndex: %d, lastLog: [%d, %d]]\n",
		len(rf.log), args.PrevLogIndex, args.PrevLogTerm, rf.lastSnapshotIndex, lastLogIndex, lastLogTerm)

	if len(args.LogEntries) > 0 && (lastLogTerm > args.LogEntries[len(args.LogEntries)-1].Term || (lastLogTerm == args.LogEntries[len(args.LogEntries)-1].Term && lastLogIndex > args.PrevLogIndex+len(args.LogEntries))) {
		reply.StaleRequest = true
		return
	}

	if len(args.LogEntries) == 0 && (lastLogTerm > args.PrevLogTerm || (lastLogTerm == args.PrevLogTerm && lastLogIndex > args.PrevLogIndex)) {
		reply.StaleRequest = true
		return
	}

	if args.PrevLogIndex < 0 {
		rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Prev log Index is negative, you  should get a snapshot in a while, buddy!!: leader %d, "+
			"\n", args.LeaderId)
		return
	}

	if args.PrevLogIndex > lastLogIndex || args.PrevLogIndex < rf.lastSnapshotIndex {
		reply.Success = false
		reply.XTerm = lastLogTerm
		reply.XIndex = lastLogIndex
		return
	} else {
		if rf.log[args.PrevLogIndex-rf.lastSnapshotIndex].Term == args.PrevLogTerm {
			reply.Success = true
			rf.log = append(rf.log[0:args.PrevLogIndex-rf.lastSnapshotIndex+1], args.LogEntries...)
		} else {
			reply.Success = false
			idx := args.PrevLogIndex

			for idx > rf.lastSnapshotIndex && idx > rf.commitIndex && rf.log[idx-rf.lastSnapshotIndex].Term == rf.log[args.PrevLogIndex-rf.lastSnapshotIndex].Term {
				idx -= 1
			}
			reply.XIndex = idx
			reply.XTerm = rf.log[idx-rf.lastSnapshotIndex].Term
		}
	}

	if reply.Success {

		if args.LeaderCommit > rf.commitIndex {
			beforeTerm := rf.commitIndex
			rf.commitIndex = minR(args.LeaderCommit, rf.getLogLengthWithSnapShotOffset()-1)
			rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "[Term: %d] Updating commit Indx of follower from %d to %d", rf.currentTerm, beforeTerm, rf.commitIndex)
			rf.notifyApplyCh <- struct{}{}
		}
		rf.persist()
	}

	rf.leaderId = args.LeaderId
	rf.resetElectionTimer()

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
// the leader
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.deferLogging("[InstallSnapshot]", "Snapshot", rf.me, time.Now())

	rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Got InstallSnapshot for the new term: [%d], leader %d\n", rf.currentTerm, args.LeaderID)
	reply.Term = rf.currentTerm

	// Update term if necessary
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	// Ensure that you follow the second rule in “Rules for Servers” before handling an incoming RPC. The second rule states:
	if rf.currentTerm < args.Term || rf.serverRole != FOLLOWER {
		reply.Term = args.Term
		rf.updateToFollowerRoleAndCatchUpNewTerm(args.Term)
	}

	rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Got something from leader install snapshot "+
		"WithOffset: [logLen: %d, argsPrevIndex: %d, argsPrevTerm: %d, lastSnapShotIndex: %d]\n", len(rf.log), args.LastIncludedIndex,
		args.LastIncludedTerm, rf.lastSnapshotIndex)

	if args.LastIncludedIndex <= rf.lastApplied || rf.lastSnapshotIndex >= args.LastIncludedIndex {
		rf.lg.DPrintfId(rf.me, serverRoleToStr(rf.serverRole), "Install snapshot not valid: [argsLastIncluded: %d, lastApplied: %d], "+
			"other condition [LastSnapshot index: %d, lastIncludedIndex :%D\n", args.LastIncludedIndex,
			rf.lastApplied, rf.lastSnapshotIndex, args.LastIncludedIndex)

		rf.mu.Unlock()
		return
	}

	start := args.LastIncludedIndex - rf.lastSnapshotIndex
	if start >= len(rf.log) {
		rf.log = []Log{{
			Term:    args.LastIncludedTerm,
			Index:   args.LastIncludedIndex,
			Command: nil,
		}}
	} else {
		rf.log = rf.log[start:]
		rf.log[0].Index = args.LastIncludedIndex
		rf.log[0].Term = args.LastIncludedTerm
	}

	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm
	rf.lastApplied = maxR(rf.lastSnapshotIndex, rf.lastApplied)

	rf.persister.SaveStateAndSnapshot(rf.getRaftStatePersistData(), args.SnapShot)

	rf.resetElectionTimer()
	rf.mu.Unlock()

	rf.applyToStateMachine <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.SnapShot,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

}
