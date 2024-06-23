package shardctrler

import (
	"bytes"
	"fmt"
	"github.com/mehulumistry/MIT-6.824-Implementation/pkg/labgob"
	"github.com/mehulumistry/MIT-6.824-Implementation/pkg/labrpc"
	"github.com/mehulumistry/MIT-6.824-Implementation/pkg/raft"
	"log"
	"sort"
	"sync"
	"time"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	killCh chan struct{}

	// Seq of configs
	configs []Config // indexed by config num
	//inMem   map[string]Config

	persister *raft.Persister
	dead      int32 // set by Kill()

	clientLastSeqNum map[int64]int64
	waitCh           map[string]chan Config
}

type Op struct {
	Operation string
	RequestId int64
	ClerkId   int64
	Value     interface{}
}

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (sc *ShardCtrler) CallRaft(op Op) Reply {
	reply := Reply{}
	reply.Timeout = false
	reply.Success = false

	_, isLeader := sc.rf.GetState()
	reply.IsLeader = isLeader

	if !isLeader {
		reply.IsLeader = false
		return reply
	}

	_, _, leader := sc.rf.Start(op)
	reply.IsLeader = leader

	if !leader {
		reply.IsLeader = false
		return reply
	}

	sc.mu.Lock()
	uniqueRequestId := fmt.Sprintf("%d:%d", op.ClerkId, op.RequestId)
	ch := make(chan Config, 1)
	sc.waitCh[uniqueRequestId] = ch
	sc.mu.Unlock()

	select {
	case value := <-ch:
		//DPrintf("Confirmed [%s] Value of key: %v, %v, id: %v post-Raft", op.Operation, op.Key, op.Value, uniqueRequestId)
		reply.Value = value
		reply.Success = true
		return reply
	case <-time.After(800 * time.Millisecond): // Timeout after 800ms if not found
		//DPrintf("[%s]Timeout - Failed to find value for key: %v, id: %v", op.Operation, op.Key, uniqueRequestId)
		sc.mu.Lock()
		delete(sc.waitCh, uniqueRequestId)
		sc.mu.Unlock()
		reply.Timeout = true
		return reply
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{
		RequestId: args.RequestId,
		Operation: "Join",
		ClerkId:   args.ClerkId,
		Value:     *args,
	}

	//DPrintf("[ShardCtrler %d][Join][REQUEST][ClerkId: %d][RequestId: %d][Args: %+v]",
	//	sc.me, args.ClerkId, args.RequestId, args) // Log before Raft call

	reply.WrongLeader = false

	result := sc.CallRaft(op)

	if result.Success {
		DPrintf("[ShardCtrler %d][Join][SUCCESS][ClerkId: %d][RequestId: %d][Config: %+v][Configs: %+v]",
			sc.me, args.ClerkId, args.RequestId, result.Value, sc.configs)
	} else if result.Timeout {
		reply.Err = "Timeout"
		DPrintf("[ShardCtrler %d][Join][TIMEOUT][ClerkId: %d][RequestId: %d][Args: %+v]",
			sc.me, args.ClerkId, args.RequestId, args)
	} else if !result.IsLeader {
		reply.WrongLeader = true
		//DPrintf("[ShardCtrler %d][Join][WRONG_LEADER][ClerkId: %d][RequestId: %d][Args: %+v]",
		//	sc.me, args.ClerkId, args.RequestId, args)
	}

}
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{
		RequestId: args.RequestId,
		Operation: "Leave",
		ClerkId:   args.ClerkId,
		Value:     *args,
	}

	//DPrintf("[ShardCtrler %d][Leave][REQUEST][ClerkId: %d][RequestId: %d][Args: %+v]",
	//	sc.me, args.ClerkId, args.RequestId, args) // Log before Raft call
	reply.WrongLeader = false

	result := sc.CallRaft(op)

	if result.Success {
		DPrintf("[ShardCtrler %d][Leave][SUCCESS][ClerkId: %d][RequestId: %d][Config: %+v]",
			sc.me, args.ClerkId, args.RequestId, result.Value)
	} else if result.Timeout {
		reply.Err = "Timeout"
		DPrintf("[ShardCtrler %d][Leave][TIMEOUT][ClerkId: %d][RequestId: %d][Args: %+v]",
			sc.me, args.ClerkId, args.RequestId, args)
	} else if !result.IsLeader {
		reply.WrongLeader = true
		//DPrintf("[ShardCtrler %d][Leave][WRONG_LEADER][ClerkId: %d][RequestId: %d][Args: %+v]",
		//	sc.me, args.ClerkId, args.RequestId, args)
	}
}
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{
		RequestId: args.RequestId,
		Operation: "Move",
		ClerkId:   args.ClerkId,
		Value:     *args,
	}
	reply.WrongLeader = false

	DPrintf("[ShardCtrler %d][Move][REQUEST][ClerkId: %d][RequestId: %d][Args: %+v]",
		sc.me, args.ClerkId, args.RequestId, args) // Log before Raft call

	result := sc.CallRaft(op)

	if result.Success {
		DPrintf("[ShardCtrler %d][Move][SUCCESS][ClerkId: %d][RequestId: %d][Config: %+v]",
			sc.me, args.ClerkId, args.RequestId, result.Value)
	} else if result.Timeout {
		reply.Err = "Timeout"
		DPrintf("[ShardCtrler %d][Move][TIMEOUT][ClerkId: %d][RequestId: %d][Args: %+v]",
			sc.me, args.ClerkId, args.RequestId, args)
	} else if !result.IsLeader {
		reply.WrongLeader = true
		//DPrintf("[ShardCtrler %d][Move][WRONG_LEADER][ClerkId: %d][RequestId: %d][Args: %+v]",
		//	sc.me, args.ClerkId, args.RequestId, args)
	}
}

func (sc *ShardCtrler) getConfigByIndex(idx int) Config {
	if idx < 0 || idx >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1].Copy()
	} else {
		return sc.configs[idx].Copy()
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {

	sc.mu.Lock()
	if args.Num > 0 && args.Num < len(sc.configs) {
		reply.Err = OK
		reply.WrongLeader = false
		reply.Config = sc.getConfigByIndex(args.Num)
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		RequestId: args.RequestId,
		Operation: "Query",
		ClerkId:   args.ClerkId,
		Value:     *args,
	}

	//DPrintf("[ShardCtrler %d][Query][REQUEST][ClerkId: %d][RequestId: %d][Num: %d]",
	//	sc.me, args.ClerkId, args.RequestId, args.Num)

	result := sc.CallRaft(op)

	if result.Success {
		reply.WrongLeader = false
		reply.Config = result.Value
		//DPrintf("[ShardCtrler %d][Query][SUCCESS][ClerkId: %d][RequestId: %d][Config: %+v]",
		//	sc.me, args.ClerkId, args.RequestId, result.Value)
	} else if result.Timeout {
		reply.Err = "Timeout"
		reply.WrongLeader = false
		//DPrintf("[ShardCtrler %d][Query][TIMEOUT][ClerkId: %d][RequestId: %d][Num: %d]",
		//	sc.me, args.ClerkId, args.RequestId, args.Num)
	} else if !result.IsLeader {
		reply.WrongLeader = true
		//DPrintf("[ShardCtrler %d][Query][WRONG_LEADER][ClerkId: %d][RequestId: %d][Num: %d]",
		//	sc.me, args.ClerkId, args.RequestId, args.Num)
	}
}

// Kill the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	close(sc.killCh)
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) readApplyCh() {
	for {
		select {
		case <-sc.killCh:
			return
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)
				sc.mu.Lock()
				sc.applyCommand(op)
				sc.mu.Unlock()
			}
		}
	}
}

func (sc *ShardCtrler) applyCommand(op Op) {
	currentRequestId := op.RequestId
	clerkId := op.ClerkId
	uniqueRequestId := fmt.Sprintf("%d:%d", clerkId, currentRequestId)

	var newConfig Config // Initialize variable to hold the new config
	switch op.Operation {
	case "Join":
		joinArgs, _ := op.Value.(JoinArgs)
		if v, ok := sc.clientLastSeqNum[op.ClerkId]; !ok || v < op.RequestId {
			newConfig = sc.rebalance(sc.configs[len(sc.configs)-1], joinArgs)
			// Update configurations and client sequence number
			sc.configs = append(sc.configs, newConfig)
			sc.clientLastSeqNum[clerkId] = op.RequestId
		}

	case "Leave":
		leaveArgs, _ := op.Value.(LeaveArgs)
		if v, ok := sc.clientLastSeqNum[op.ClerkId]; !ok || v < op.RequestId {
			newConfig = sc.rebalance(sc.configs[len(sc.configs)-1], leaveArgs)
			// Update configurations and client sequence number
			sc.configs = append(sc.configs, newConfig)
			sc.clientLastSeqNum[clerkId] = op.RequestId
		}

	case "Move":
		moveArgs, _ := op.Value.(MoveArgs)
		if v, ok := sc.clientLastSeqNum[op.ClerkId]; !ok || v < op.RequestId {
			newConfig = sc.rebalance(sc.configs[len(sc.configs)-1], moveArgs)
			// Update configurations and client sequence number
			sc.configs = append(sc.configs, newConfig)
			sc.clientLastSeqNum[clerkId] = op.RequestId
		}
	case "Query":
		queryArgs, _ := op.Value.(QueryArgs)
		newConfig = sc.getConfigByIndex(queryArgs.Num)

	default:
		fmt.Printf("Unknown operation: %s\n", op.Operation)
		return
	}

	// Send the config through the channel if it's a Query operation

	if ch, ok := sc.waitCh[uniqueRequestId]; ok {
		ch <- newConfig
		close(ch)
		delete(sc.waitCh, uniqueRequestId)
	}

}

func (sc *ShardCtrler) rebalance(oldConfig Config, args interface{}) (newConfig Config) {
	newConfig = oldConfig.Copy()
	newConfig.Num++
	//DPrintf("[ShrdCtr: %d]Rebalance starting with oldConfig: %+v, args: %+v", sc.me, oldConfig, args) // Log initial state

	switch v := args.(type) {
	case JoinArgs:
		// Handle group joining (same as before)
		DPrintf("[ShrdCtr: %d]Join operation: adding GIDs: %+v", sc.me, v)

		for gid, servers := range v.Servers {
			newConfig.Groups[gid] = servers
		}
	case LeaveArgs:
		// Handle group leaving (updated)
		DPrintf("[ShrdCtr: %d]Leave operation: removing GIDs: %v", sc.me, v.GIDs)
		for _, gid := range v.GIDs {
			delete(newConfig.Groups, gid)
		}

	case MoveArgs:
		// Handle shard movement
		DPrintf("[ShrdCtr: %d] Move operation: moving shard %d to GID %d", sc.me, v.Shard, v.GID)

		if v.Shard < 0 || v.Shard >= len(newConfig.Shards) {
			panic("Invalid shard ID")
		}
		if _, exists := newConfig.Groups[v.GID]; !exists {
			panic("Target GID does not exist")
		}
		newConfig.Shards[v.Shard] = v.GID
		return newConfig
	default:
		panic("Invalid arguments type") // Handle incorrect argument type
	}

	numGroups := len(newConfig.Groups)

	if numGroups == 0 { //Handle case where no groups left after leaving
		for i := 0; i < len(oldConfig.Shards); i++ {
			newConfig.Shards[i] = 0
		}
		return
	}
	expectedDistribution := getShardDistribution(numGroups)
	//DPrintf("New distribution %+v\n", expectedDistribution)
	// Map replica group IDs to distribution numbers
	gids := make([]int, 0, len(newConfig.Groups))
	for gid := range newConfig.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	// Map replica group IDs to distribution numbers
	gidToDistNum := make(map[int]int)
	distNum := 1
	for _, gid := range gids { // Iterate over sorted GIDs
		gidToDistNum[distNum] = gid
		distNum++
	}

	//DPrintf("gidToDistNum %+v\n", gidToDistNum)

	for i := 0; i < len(newConfig.Shards); i++ {
		newConfig.Shards[i] = gidToDistNum[expectedDistribution[i]]
	}
	//DPrintf("[ShrdCtr: %d]Rebalance completed. New config: %+v", sc.me, newConfig) // Log final config

	return newConfig
}

func getShardDistribution(numGroups int) [NShards]int {
	if numGroups <= 0 {
		panic("Number of groups must be positive")
	}

	switch numGroups {
	case 1:
		return [NShards]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	case 2:
		return [NShards]int{1, 1, 1, 1, 1, 2, 2, 2, 2, 2}
	case 3:
		return [NShards]int{1, 1, 1, 1, 2, 2, 2, 3, 3, 3}
	case 4:
		return [NShards]int{1, 2, 3, 4, 1, 2, 3, 4, 1, 2}
	case 5:
		return [NShards]int{1, 2, 3, 4, 5, 1, 2, 3, 4, 5}
	case 6:
		return [NShards]int{1, 2, 3, 4, 5, 6, 1, 2, 3, 4}
	case 7:
		return [NShards]int{1, 2, 3, 4, 5, 6, 7, 1, 2, 3}
	case 8:
		return [NShards]int{1, 2, 3, 4, 5, 6, 7, 8, 1, 2}
	case 9:
		return [NShards]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 1}
	case 10:
		return [NShards]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	default:
		DPrintf("🤷 Oops! We only have hardcoded shard distributions for up to 10 groups. Keeping the existing configuration. 😊")
		return [NShards]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	}
}

// StartServer servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.killCh = make(chan struct{})

	sc.configs = make([]Config, 1)

	// Initialize all the shards to 0
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Num = 0

	labgob.Register(Op{})
	labgob.Register(Reply{})
	labgob.Register(Config{})
	labgob.Register(QueryArgs{})
	labgob.Register(QueryReply{})
	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveReply{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.MakeRaft("ShardCtlr", servers, me, persister, sc.applyCh)
	sc.killCh = make(chan struct{})

	sc.clientLastSeqNum = make(map[int64]int64)

	sc.waitCh = make(map[string]chan Config)
	sc.persister = persister

	sc.recover(nil)
	go sc.readApplyCh()

	return sc
}

func (sc *ShardCtrler) recover(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvData []Config
	var seenIds map[int64]int64

	//DPrintf("ShrdCtrlr Recovering... %d", sc.me)
	if d.Decode(&kvData) != nil || d.Decode(&seenIds) != nil {
		log.Fatal("kv recover err")
	} else {
		sc.configs = kvData
		sc.clientLastSeqNum = seenIds
	}
}
func (sc *ShardCtrler) encodeKVData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(sc.configs); err != nil {
		panic(err)
	}
	if err := e.Encode(sc.clientLastSeqNum); err != nil {
		panic(err)
	}
	data := w.Bytes()
	return data
}
