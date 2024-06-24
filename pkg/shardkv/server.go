package shardkv

import (
	"bytes"
	"fmt"
	"github.com/mehulumistry/MIT-6.824-Implementation/pkg/labgob"
	"github.com/mehulumistry/MIT-6.824-Implementation/pkg/labrpc"
	"github.com/mehulumistry/MIT-6.824-Implementation/pkg/raft"
	"github.com/mehulumistry/MIT-6.824-Implementation/pkg/shardctrler"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	Operation string
	RequestId int64
	ClerkId   int64

	// Key-Value operation fields
	Key   string
	Value string

	// Shard migration fields
	Shard         int
	ShardStatus   string
	Data          map[string]string
	ClientLastSeq map[int64]int64

	// Configuration update fields
	ShardCfg       shardctrler.Config
	ShardCfgNum    int
	ShardStatusMap map[int]string
}

// Running, InMigration, UnAvailable (tombstone?)
// Have pre defined map with pre defined states and update keys in there.
// start 1st config with running... and later default it with in-migration and unavailble(err wrong group).
// push data

/*
TODO:

1) push ---> delete upgrade
2) We are running cfg from 1 ---> N. Even if you join at cfg 3 you start from 1 and go until 3





*/

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	mck          *shardctrler.Clerk
	waitCh       map[string]chan string // uniqueRequestId -> (shard -> chan string)
	dead         int32                  // set by Kill()
	killCh       chan struct{}

	// Your definitions here.
	persister *raft.Persister

	clientLastSeqNum map[int64]int64
	inMem            map[int]map[string]string // shard -> (key -> value)
	shardStatus      map[int]string            // shard -> Running, InMigration, UnAvailable
	shardSeqNum      int64
	clerkId          int64
	sctrlerCfg       shardctrler.Config
}

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	PullConfigInterval = time.Millisecond * 80
)

func (kv *ShardKV) CallRaft(op Op) Reply {
	reply := Reply{}
	reply.Success = false
	reply.Value = ""

	_, isLeader := kv.rf.GetState()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return reply
	}

	_, _, leader := kv.rf.Start(op)

	if !leader {
		reply.Err = ErrWrongLeader
		return reply
	}

	kv.mu.Lock()
	uniqueRequestId := fmt.Sprintf("%d:%d", op.ClerkId, op.RequestId)
	ch := make(chan string, 1)
	kv.waitCh[uniqueRequestId] = ch
	kv.mu.Unlock()

	select {
	case value := <-ch:
		DPrintf("[GID: %d][KVSERVER: %d]Confirmed [%s] Value of key: %v, %v, id: %v post-Raft",
			kv.gid, kv.me, op.Operation, op.Key, value, uniqueRequestId)
		if strings.Contains(value, "Err") {
			reply.Success = false
			reply.Err = value
			return reply
		}
		reply.Value = value
		reply.Success = true
		return reply
	case <-time.After(5000 * time.Millisecond): // Timeout after 800ms if not found
		//DPrintf("[%s]Timeout - Failed to find value for key: %v, id: %v", op.Operation, op.Key, uniqueRequestId)
		kv.mu.Lock()
		delete(kv.waitCh, uniqueRequestId)
		kv.mu.Unlock()
		reply.Err = ErrTimeOut
		return reply
	}
}

func (kv *ShardKV) pullConfig() {
	// Initialize with an invalid config number
	for {
		select {
		case <-kv.killCh:
			return
		default:
			if _, isLeader := kv.rf.GetState(); !isLeader {
				//DPrintf("[ShardKV %d] Not a leader, sleeping for %v", kv.me, PullConfigInterval)
				time.Sleep(PullConfigInterval)
				continue
			}

			//DPrintf("[ShardKV %d] Checking shard statuses for 'wait' state", kv.me)
			kv.mu.Lock()

			//DPrintf("[ShardKV %d] Checking shard statuses for 'push' state", kv.me)
			if kv.checkShardStatus("push") {
				DPrintf("[ShardKV %d][GID %d]  Found shard in 'push' state, processing pending migrations, %+v", kv.me, kv.gid, kv.shardStatus)
				// Unlocked inside
				kv.processPendingMigrations()
				time.Sleep(PullConfigInterval)
				continue
			}

			if kv.checkShardStatus("wait") {
				DPrintf("[ShardKV %d][GID %d]  Found shard in 'wait' state, sleeping  %+v", kv.me, kv.gid, kv.shardStatus)
				kv.mu.Unlock()
				time.Sleep(PullConfigInterval)
				continue
			}

			cfgNumToQuery := kv.sctrlerCfg.Num + 1
			//DPrintf("[ShardKV %d] Querying new config from controller with config number %d", kv.me, cfgNumToQuery)
			kv.mu.Unlock()

			newCfg := kv.mck.Query(cfgNumToQuery)

			//DPrintf("[ShardKV %d] Acquired new config %d", kv.me, newCfg.Num)
			kv.mu.Lock()

			if shouldUpdate, op := kv.shouldUpdateConfig(newCfg); shouldUpdate {
				DPrintf("[ShardKV %d][GID %d]  Should update config to %d", kv.me, kv.gid, newCfg.Num)
				kv.mu.Unlock()
				if err := kv.applyConfigUpdate(op); err != nil {
					DPrintf("[ShardKV %d][GID %d]  Error applying config update: %v", kv.me, kv.gid, err)
					time.Sleep(PullConfigInterval)
					continue
				}
				// BUG: If no err should we direcly start the migration?
			} else {
				kv.mu.Unlock()
			}

			time.Sleep(PullConfigInterval)
		}
	}
}

func (kv *ShardKV) shouldUpdateConfig(newCfg shardctrler.Config) (bool, Op) {
	// Only update if the config is newer than the currently applied one
	if newCfg.Num > kv.sctrlerCfg.Num || newCfg.Num == 0 {
		newShardStatus := kv.calculateNewShardStatus(newCfg)
		DPrintf("[ShardKV %d][GID %d][ConfigNum: %d]  Calculated new shard status: %+v", kv.me, kv.gid, newCfg.Num, newShardStatus)

		if newCfg.Num > kv.sctrlerCfg.Num {
			DPrintf("[ShardKV %d][GID %d]  Shard status has changed or new config number is higher", kv.me, kv.gid)
			return true, Op{
				Operation:      "UpdateConfig",
				ShardCfg:       newCfg,
				ShardStatusMap: newShardStatus,
				RequestId:      kv.shardSeqNum + 1,
				ClerkId:        kv.clerkId,
				ShardCfgNum:    newCfg.Num,
			}
		}
	}
	//DPrintf("[ShardKV %d][GID %d] No update needed for config", kv.me, kv.gid)
	return false, Op{}
}
func (kv *ShardKV) applyConfigUpdate(op Op) error {
	reply := kv.CallRaft(op)
	if !reply.Success {
		DPrintf("[ShardKV %d][GID %d]  Failed to apply config update Err: %s", kv.me, kv.gid, reply.Err)

		return fmt.Errorf("failed to apply config update")
	}
	DPrintf("[ShardKV %d][GID %d]  Successfully applied config update", kv.me, kv.gid)
	return nil
}
func (kv *ShardKV) calculateNewShardStatus(newCfg shardctrler.Config) map[int]string {
	oldCfg := kv.sctrlerCfg.Copy()
	newShardStatus := make(map[int]string)

	if newCfg.Num == 1 {
		DPrintf("[ShardKV %d][GID %d]  Processing first config or catch-up query", kv.me, kv.gid)
		for shard := range newCfg.Shards {
			if newCfg.Shards[shard] == kv.gid && isValidStatusTransition(kv.shardStatus[shard], "running") {
				newShardStatus[shard] = "running"
			} else {
				newShardStatus[shard] = kv.shardStatus[shard]
			}
		}
	} else {
		for shard := range newCfg.Shards {
			if newCfg.Shards[shard] != kv.gid && oldCfg.Shards[shard] == kv.gid {
				if isValidStatusTransition(kv.shardStatus[shard], "push") {
					newShardStatus[shard] = "push"
					DPrintf("[ShardKV %d][GID %d]  Shard %d marked for push", kv.me, kv.gid, shard)
				}
			} else if oldCfg.Shards[shard] != kv.gid && newCfg.Shards[shard] == kv.gid {
				if isValidStatusTransition(kv.shardStatus[shard], "wait") {
					newShardStatus[shard] = "wait"
					DPrintf("[ShardKV %d] [GID %d]  Shard %d marked for wait", kv.me, kv.gid, shard)
				}
			} else {
				newShardStatus[shard] = kv.shardStatus[shard]
			}
		}
	}

	DPrintf("[ShardKV %d][GID: %v] New shard status: %+v", kv.me, kv.gid, newShardStatus)
	return newShardStatus
}
func (kv *ShardKV) checkShardStatus(status string) bool {
	for _, s := range kv.shardStatus {
		if s == status {
			DPrintf("[ShardKV %d] [GID %d] Found shard in '%s' status", kv.me, kv.gid, status)
			return true
		}
	}
	return false
}
func (kv *ShardKV) migrateShard(args *MigrateShardArgs, cfg shardctrler.Config, shard int, startIndex int) int {
	reply := &MigrateShardReply{}

	servers, ok := cfg.Groups[cfg.Shards[shard]]
	if !ok {
		return -1
	}

	serverCount := len(servers)
	for i := 0; i < serverCount; i++ {
		si := (startIndex + i) % serverCount
		srv := kv.make_end(servers[si])
		DPrintf("[ShardKV %d] [GID %d] [TO_ShardKV: %d] Starting migration of shard %d to GID %d", kv.me, kv.gid, si, args.Shard, cfg.Shards[shard]) // Initial log

		res := srv.Call("ShardKV.ReceiveShard", args, reply)
		if res && reply.Err == OK {
			DPrintf("[ShardKV %d] [GID %d] Migration of shard %d to GID %d successful, calling raft...", kv.me, kv.gid, args.Shard, cfg.Shards[shard])
			return si // Return the index of the successful server
		} else {
			DPrintf("[ShardKV %d] [GID %d] Migration of shard %d to GID %d failed: %v", kv.me, kv.gid, args.Shard, cfg.Shards[shard], reply.Err)
		}
	}

	time.Sleep(50 * time.Millisecond) // Wait before retrying
	return -1
}
func (kv *ShardKV) migrateShardOld(args *MigrateShardArgs, cfg shardctrler.Config, newGID int) {

	// BUG: What if this never succeeds or fails right after one RPC call?
	reply := &MigrateShardReply{}
	maxRetries := 5
	retryCount := 0

	for {
		if retryCount >= maxRetries {
			DPrintf("[ShardKV %d] [GID %d] Max retries reached for migrating shard %d to GID %d. Aborting.", kv.me, kv.gid, args.Shard, newGID)
			return
		}

		if servers, ok := cfg.Groups[newGID]; ok {
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				DPrintf("[ShardKV %d] [GID %d] [TO_ShardKV: %d] Starting migration of shard %d to GID %d", kv.me, kv.gid, si, args.Shard, newGID) // Initial log

				res := srv.Call("ShardKV.ReceiveShard", args, reply)
				if res && reply.Err == OK {
					DPrintf("[ShardKV %d] [GID %d] Migration of shard %d to GID %d successful, calling raft...", kv.me, kv.gid, args.Shard, newGID)
					return
				} else {
					DPrintf("[ShardKV %d] [GID %d] Migration of shard %d to GID %d failed: %v", kv.me, kv.gid, args.Shard, newGID, reply.Err)
				}
			}
		}
		retryCount++
		time.Sleep(50 * time.Millisecond) // Wait before retrying
	}
}

func (kv *ShardKV) processPendingMigrations() {
	DPrintf("[ShardKV %d][GID: %d] Processing pending migrations", kv.me, kv.gid)

	shardsToMigrate := make(map[int]*MigrateShardArgs)
	for shard, status := range kv.shardStatus {
		if status == "push" {
			shardsToMigrate[shard] = &MigrateShardArgs{
				ClerkId:          kv.clerkId,
				RequestId:        kv.shardSeqNum + 1,
				Shard:            shard,
				Data:             deepCopyMap(kv.inMem[shard]),
				ConfigNum:        kv.sctrlerCfg.Num,
				ClientLastSeqNum: deepCopyMapInt64(kv.clientLastSeqNum),
				GID:              kv.gid,
			}
		}
	}

	cfg := kv.sctrlerCfg

	kv.mu.Unlock()

	// TODO: Starting from cfg:1 and looping over.
	//startIndex := 0 // Start from the first server index
	for shard, args := range shardsToMigrate {

		// BUG: What if there is a failure here, whole group is down/left? this
		// will keep retrying....
		kv.migrateShardOld(args, cfg, kv.sctrlerCfg.Shards[shard])

		opUpdateShard := Op{
			Operation:   "UpdateShard",
			ShardStatus: "migrated",
			RequestId:   args.RequestId,
			ClerkId:     args.ClerkId,
			ShardCfgNum: args.ConfigNum,
			Shard:       shard,
		}

		replyUpdateShard := kv.CallRaft(opUpdateShard)
		if !replyUpdateShard.Success {
			DPrintf("[ERROR][GID: %d] ShardKV %d: ErrWrongLeader while replyUpdateShard. Retrying %s", kv.gid, kv.me, replyUpdateShard.Err)
		}
	}
}

func isValidStatusTransition(currentStatus, newStatus string) bool {
	validTransitions := map[string][]string{
		"wait":        {"running"},  // Intermediate state
		"push":        {"migrated"}, // Intermediate state
		"running":     {"push"},
		"migrated":    {"wait"},
		"unavailable": {"wait", "running"},
	}

	validNextStatuses, exists := validTransitions[currentStatus]
	if !exists {
		return false
	}

	for _, status := range validNextStatuses {
		if status == newStatus {
			return true
		}
	}
	return false
}

func deepCopyMap(m map[string]string) map[string]string {
	newMap := make(map[string]string, len(m))
	for k, v := range m {
		newMap[k] = v
	}
	return newMap
}
func deepCopyMapInt(m map[int]string) map[int]string {
	newMap := make(map[int]string, len(m))
	for k, v := range m {
		newMap[k] = v
	}
	return newMap
}

// Helper function to deep copy a map[int64]int64
func deepCopyMapInt64(m map[int64]int64) map[int64]int64 {
	newMap := make(map[int64]int64, len(m))
	for k, v := range m {
		newMap[k] = v
	}
	return newMap
}
func (kv *ShardKV) readApplyCh() {
	for {
		select {
		case <-kv.killCh:
			return
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)

				kv.applyCommand(op)

				if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
					kv.rf.Snapshot(msg.CommandIndex, kv.encodeKVData())
				}

			}
			if msg.SnapshotValid {
				kv.mu.Lock()
				kv.recover(msg.Snapshot)
				kv.mu.Unlock()
			}
		}
	}
}

/*
We need synchronization on commands so we start with
ShardInit ---> ShardConfig (Increment the config by 1) ---> ShardChange  ---> ShardLeave ---> ShardJoin
*/
func (kv *ShardKV) applyCommand(op Op) {
	kv.mu.Lock()
	currentRequestId := op.RequestId
	clerkId := op.ClerkId
	uniqueRequestId := fmt.Sprintf("%d:%d", clerkId, currentRequestId)
	currentKey := op.Key
	var response string = ""

	switch op.Operation {
	case "Put":
		if kv.shardStatus[op.Shard] == "running" {
			if v, ok := kv.clientLastSeqNum[op.ClerkId]; !ok || v < op.RequestId {
				kv.inMem[key2shard(op.Key)][currentKey] = op.Value
				kv.clientLastSeqNum[op.ClerkId] = op.RequestId
				response = kv.inMem[key2shard(op.Key)][currentKey]
			}
		} else {
			response = ErrWrongGroup
		}
	case "Append":
		if kv.shardStatus[op.Shard] == "running" {
			if v, ok := kv.clientLastSeqNum[op.ClerkId]; !ok || v < op.RequestId {
				existingVal, exists := kv.inMem[key2shard(op.Key)][currentKey]
				if !exists {
					existingVal = ""
				}
				kv.inMem[key2shard(op.Key)][currentKey] = existingVal + op.Value
				kv.clientLastSeqNum[op.ClerkId] = op.RequestId
				response = kv.inMem[key2shard(op.Key)][currentKey]
			}
		} else {
			response = ErrWrongGroup
		}
	case "Get":
		if kv.shardStatus[op.Shard] != "running" {
			response = ErrWrongGroup
		} else {
			response = kv.inMem[key2shard(op.Key)][currentKey]
		}
	case "UpdateShard":
		// Only update if the operation's ShardCfgNum matches the current configuration number
		if op.ShardCfgNum == kv.sctrlerCfg.Num {
			currentStatus := kv.shardStatus[op.Shard]

			if isValidStatusTransition(currentStatus, op.ShardStatus) {
				kv.shardStatus[op.Shard] = op.ShardStatus

				// Clear shard data if the shard status is being set to "migrated"
				if op.ShardStatus == "migrated" {
					kv.inMem[op.Shard] = make(map[string]string)
					DPrintf("[KVServer: %d][GID: %d] Shard %d data cleared due to migration", kv.me, kv.gid, op.Shard)
				}
			} else {
				response = ErrWrongGroup
				DPrintf("[KVServer: %d][GID: %d] Invalid status transition for shard %d: %s -> %s", kv.me, kv.gid, op.Shard, currentStatus, op.ShardStatus)
			}
		} else {
			response = ErrWrongConfig
		}
	case "MigrateShard":
		// Check if the operation's ShardCfgNum matches the current configuration number
		if op.ShardCfgNum == kv.sctrlerCfg.Num {
			// Ensure the shard status is "wait" before accepting the migration
			if isValidStatusTransition(kv.shardStatus[op.Shard], "running") {
				// Apply the migration data if the clientLastSeqNum is not smaller
				for clientId, seqNum := range op.ClientLastSeq {
					if seqNum >= kv.clientLastSeqNum[clientId] {
						kv.clientLastSeqNum[clientId] = seqNum
					} else {
						// If the seqNum is smaller, reject the migration for this client
						DPrintf("[KVServer: %d][GID: %d] Rejecting migration for client %d due to older seqNum: %d < %d", kv.me, kv.gid, clientId, seqNum, kv.clientLastSeqNum[clientId])
						continue
					}
				}
				// Apply the migration data
				for k, v := range op.Data {
					kv.inMem[op.Shard][k] = v
				}
				kv.shardStatus[op.Shard] = "running"
				DPrintf("[KVServer: %d][GID: %d] Shard %d set to running", kv.me, kv.gid, op.Shard)
			} else {
				// If the shard status is not "wait", reject the migration
				response = ErrWrongGroup
				DPrintf("[KVServer: %d][GID: %d] Rejecting migration for shard %d because it is not in 'wait' state", kv.me, kv.gid, op.Shard)
			}
		} else {
			response = ErrWrongConfig
			DPrintf("[KVServer: %d][GID: %d] Rejecting migration for shard %d due to mismatched config number: %d != %d", kv.me, kv.gid, op.Shard, op.ShardCfgNum, kv.sctrlerCfg.Num)
		}
	case "UpdateConfig":
		newCfg := op.ShardCfg

		// Check if the config is newer
		if newCfg.Num > kv.sctrlerCfg.Num {
			// Update kv.sctrlerCfg
			kv.sctrlerCfg = newCfg.Copy() // Note: ensure deep copy if necessary
			kv.shardStatus = deepCopyMapInt(op.ShardStatusMap)

			DPrintf("[ShardKV %d] [GID %d] Successfully applied new config: %+v", kv.me, kv.gid, newCfg)
		} else {
			response = ErrWrongConfig
			DPrintf("[ShardKV %d] [GID %d] Ignoring stale config: %+v", kv.me, kv.gid, newCfg)
		}

	default:
		fmt.Printf("Unknown operation: %s\n", op.Operation) // Or log the error
	}

	if ch, ok := kv.waitCh[uniqueRequestId]; ok {
		ch <- response
		close(ch)
		delete(kv.waitCh, uniqueRequestId)
	}

	kv.mu.Unlock()

}

func (kv *ShardKV) recover(data []byte) {
	if data == nil || len(data) < 1 {
		data = kv.persister.ReadSnapshot()
	}

	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	// Decode existing fields
	var kvData map[int]map[string]string
	var seenIds map[int64]int64

	// Decode new fields
	var shardStatus map[int]string
	var shardSeqNum int64
	var clerkId int64
	var sctrlerCfg shardctrler.Config

	if d.Decode(&kvData) != nil ||
		d.Decode(&seenIds) != nil ||
		d.Decode(&shardStatus) != nil ||
		d.Decode(&shardSeqNum) != nil ||
		d.Decode(&clerkId) != nil ||
		d.Decode(&sctrlerCfg) != nil {
		log.Fatal("kv recover err")
	} else {
		kv.inMem = kvData
		kv.clientLastSeqNum = seenIds

		// Set the recovered values for new fields
		kv.shardStatus = shardStatus
		kv.shardSeqNum = shardSeqNum
		kv.clerkId = clerkId
		kv.sctrlerCfg = sctrlerCfg
	}
}
func (kv *ShardKV) encodeKVData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// Encode existing fields
	if err := e.Encode(kv.inMem); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.clientLastSeqNum); err != nil {
		panic(err)
	}

	// Encode new fields
	if err := e.Encode(kv.shardStatus); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.shardSeqNum); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.clerkId); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.sctrlerCfg); err != nil {
		panic(err)
	}

	data := w.Bytes()
	return data
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {

	kv.mu.Lock()

	shard := key2shard(args.Key)
	gid := kv.sctrlerCfg.Shards[shard]

	if _, ok := kv.sctrlerCfg.Groups[gid]; !ok || gid != kv.gid && gid != 0 {
		DPrintf("[GID: %d][KVServer %d][GET][WRONG_ERROR_GROUP][ClerkId: %d][RequestId: %d][Args: %+v][Config: %+v][Shard: %d]",
			kv.gid, gid, args.ClerkId, args.RequestId, args.Key, kv.sctrlerCfg.Num, key2shard(args.Key)) // Log before Raft call
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if kv.shardStatus[shard] != "running" {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		DPrintf("[GID: %d][KVServer %d][GET][ShardUnAvailable][ClerkId: %d][RequestId: %d][Args: %+v][Config: %+v][Shard: %d][Status: %s]",
			kv.gid, kv.me, args.ClerkId, args.RequestId, args.Key, kv.sctrlerCfg.Num, shard, kv.shardStatus[shard]) // Log before Raft call
		return
	}

	kv.mu.Unlock()

	op := Op{
		RequestId: args.RequestId,
		Key:       args.Key,
		Operation: "Get",
		ClerkId:   args.ClerkId,
		Shard:     shard,
	}

	result := kv.CallRaft(op)

	if result.Success {
		DPrintf("[GID: %d][KV_GID: %d][KVServer %d][GET][Key: %s][SUCCESS][ClerkId: %d][RequestId: %d][GetValue: %+v][Config: %+v][Shard:  %d]",
			kv.gid, gid, kv.me, args.Key, args.ClerkId, args.RequestId, result.Value, kv.sctrlerCfg, key2shard(args.Key))
		reply.Value = result.Value
		reply.Err = OK
	} else {
		reply.Err = result.Err
	}
}
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	shard := key2shard(args.Key)
	gid := kv.sctrlerCfg.Shards[shard]

	// KV GID should be 101 because shard is 7
	if _, ok := kv.sctrlerCfg.Groups[gid]; !ok || gid != kv.gid && gid != 0 {
		DPrintf("[GID: %d][EXPECTED_GID: %d][KVServer %d][PUT][WRONG_ERROR_GROUP][ClerkId: %d][RequestId: %d][Args: %+v][Config: %+v][Shard: %d]",
			kv.gid, gid, kv.me, args.ClerkId, args.RequestId, args, kv.sctrlerCfg, key2shard(args.Key)) // Log before Raft call
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if kv.shardStatus[shard] != "running" {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		DPrintf("[GID: %d][KVServer %d][PUT][ShardUnAvailable][ClerkId: %d][RequestId: %d][Args: %+v][Config: %+v][Shard: %d][Status: %s]",
			kv.gid, kv.me, args.ClerkId, args.RequestId, args, kv.sctrlerCfg, shard, kv.shardStatus[shard]) // Log before Raft call
		return
	}

	kv.mu.Unlock()

	op := Op{
		RequestId: args.RequestId,
		Key:       args.Key,
		Operation: args.Op,
		Value:     args.Value,
		ClerkId:   args.ClerkId,
		Shard:     shard,
	}

	//DPrintf("[GID: %d][KVServer %d][PUT][REQUEST][ClerkId: %d][RequestId: %d][Args: %+v]",
	//	kv.gid, kv.me, args.ClerkId, args.RequestId, args) // Log before Raft call

	result := kv.CallRaft(op)

	if result.Success {
		DPrintf("[GID: %d][KVServer %d][%s][SUCCESS][KEY: %s][VALUE: %s][ClerkId: %d][RequestId: %d][Shard: %d][Status: %+v]",
			kv.gid, kv.me, args.Op, args.Key, args.Value, args.ClerkId, args.RequestId, key2shard(args.Key), kv.shardStatus)
		reply.Err = OK
	} else {
		reply.Err = result.Err
		//DPrintf("[GID: %d][KVServer %d][PUT][WRONG_LEADER][ClerkId: %d][RequestId: %d][Args: %+v]",
		//	kv.gid, kv.me, args.ClerkId, args.RequestId, args)
	}
}
func (kv *ShardKV) ReceiveShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	// Check configuration number to ensure shard migration is valid
	kv.mu.Lock()
	if args.ConfigNum != kv.sctrlerCfg.Num {
		reply.Err = ErrWrongConfig
		DPrintf("[GID: %d][FROM_GID: %d][KVServer %d][ReceiveShard][ErrWrongConfig][ClerkId: %d][RequestId: %d][ExpectedConfigNum: %+v][ConfigNum: %+v][Shard: %d]",
			kv.gid, args.GID, kv.me, args.ClerkId, args.RequestId, args.ConfigNum, kv.sctrlerCfg.Num, args.Shard) // Log before Raft call
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		ClerkId:       args.ClerkId,
		RequestId:     args.RequestId,
		Operation:     "MigrateShard",
		Shard:         args.Shard,
		Data:          args.Data,
		ClientLastSeq: args.ClientLastSeqNum,
		ShardCfgNum:   args.ConfigNum,
	}

	result := kv.CallRaft(op)

	if result.Success {
		DPrintf("[ShardKV %d] [GID %d] Successfully received shard %d from GID %d (config: %+v, data: %+v)",
			kv.me, kv.gid, args.Shard, args.GID, kv.sctrlerCfg, args.Data)
		reply.Err = OK
	} else {
		reply.Err = result.Err
		DPrintf("[ShardKV %d] [GID %d] Shard %d migration failed: %s",
			kv.me, kv.gid, args.Shard, result.Err)
	}

}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	close(kv.killCh)
}
func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(
	servers []*labrpc.ClientEnd,
	me int,
	persister *raft.Persister,
	maxraftstate int,
	gid int,
	ctrlers []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd,
) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(MigrateShardArgs{})
	labgob.Register(MigrateShardReply{})
	labgob.Register(Reply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.MakeRaft(fmt.Sprintf("KVReplica: %d", me), servers, me, persister, kv.applyCh)

	kv.killCh = make(chan struct{})

	kv.clientLastSeqNum = make(map[int64]int64)

	kv.clerkId = nrand()
	kv.shardSeqNum = 0
	kv.inMem = make(map[int]map[string]string)
	kv.shardStatus = make(map[int]string)
	kv.waitCh = make(map[string]chan string)
	for shard := 0; shard < shardctrler.NShards; shard++ {
		kv.shardStatus[shard] = "unavailable"
		kv.inMem[shard] = make(map[string]string)
	}

	kv.sctrlerCfg = shardctrler.Config{
		Num: 0,
	}
	kv.persister = persister

	kv.recover(nil)
	go kv.readApplyCh()
	go kv.pullConfig()

	return kv
}
