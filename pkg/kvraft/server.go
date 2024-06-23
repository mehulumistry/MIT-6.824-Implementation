package kvraft

import (
	"bytes"
	"fmt"
	"github.com/mehulumistry/MIT-6.824-Implementation/pkg/labgob"
	"github.com/mehulumistry/MIT-6.824-Implementation/pkg/labrpc"
	"github.com/mehulumistry/MIT-6.824-Implementation/pkg/raft"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
func DPrintfId(requestId string, id int, role string, format string, a ...interface{}) (n int, err error) {
	if Debug {
		strId := time.Now().String() + " [SERVER_ID:" + "[" + strconv.Itoa(id) + "][" + role + "][RequestId:" + requestId + "]"
		msg := fmt.Sprintf(strId+format, a...)
		//utils.logger.LogLocalEvent(msg, govec.GetDefaultLogOptions())
		fmt.Println(msg)
	}
	return
}

type Op struct {
	Operation string
	RequestId int64
	ClerkId   int64
	Key       string
	Value     string
}

type KVServer struct {
	mu sync.Mutex
	me int
	rf *raft.Raft

	persister        *raft.Persister
	applyCh          chan raft.ApplyMsg
	dead             int32 // set by Kill()
	killCh           chan struct{}
	maxraftstate     int // snapshot if log grows this big
	inMem            map[string]string
	clientLastSeqNum map[int64]int64
	waitCh           map[string]chan string
}

func (kv *KVServer) CallRaft(op Op, reply *Reply) {

	reply.Timeout = false
	reply.Success = false
	reply.Value = ""

	_, isLeader := kv.rf.GetState()
	reply.IsLeader = isLeader

	if !isLeader {
		reply.IsLeader = false
		return
	}

	_, _, leader := kv.rf.Start(op)
	reply.IsLeader = leader

	if !leader {
		reply.IsLeader = false
		return
	}

	kv.mu.Lock()
	uniqueRequestId := fmt.Sprintf("%d:%d", op.ClerkId, op.RequestId)
	ch := make(chan string, 1)
	kv.waitCh[uniqueRequestId] = ch
	kv.mu.Unlock()

	select {
	case value := <-ch:
		//DPrintf("Confirmed [%s] Value of key: %v, %v, id: %v post-Raft", op.Operation, op.Key, op.Value, uniqueRequestId)
		reply.Value = value
		reply.Success = true

	case <-time.After(800 * time.Millisecond): // Timeout after 800ms if not found
		//DPrintf("[%s]Timeout - Failed to find value for key: %v, id: %v", op.Operation, op.Key, uniqueRequestId)
		kv.mu.Lock()
		delete(kv.waitCh, uniqueRequestId)
		kv.mu.Unlock()
		reply.Timeout = true
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *Reply) {
	op := Op{
		RequestId: args.RequestId,
		Key:       args.Key,
		Operation: "Get",
		ClerkId:   args.ClerkId,
	}

	kv.CallRaft(op, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *Reply) {
	op := Op{
		RequestId: args.RequestId,
		Key:       args.Key,
		Operation: args.Op,
		Value:     args.Value,
		ClerkId:   args.ClerkId,
	}
	kv.CallRaft(op, reply)
}

// Kill the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	close(kv.killCh)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) readApplyCh() {
	for {
		select {
		case <-kv.killCh:
			return
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)
				kv.mu.Lock()
				kv.applyCommand(op)
				kv.mu.Unlock()

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

func (kv *KVServer) applyCommand(op Op) {
	currentRequestId := op.RequestId
	clerkId := op.ClerkId
	uniqueRequestId := fmt.Sprintf("%d:%d", clerkId, currentRequestId)

	currentKey := op.Key

	switch op.Operation {
	case "Put":
		if v, ok := kv.clientLastSeqNum[op.ClerkId]; !ok || v < op.RequestId {
			kv.inMem[currentKey] = op.Value
			kv.clientLastSeqNum[op.ClerkId] = op.RequestId
		}
	case "Append":
		if v, ok := kv.clientLastSeqNum[op.ClerkId]; !ok || v < op.RequestId {
			existingVal, exists := kv.inMem[currentKey]
			if !exists {
				existingVal = ""
			}
			kv.inMem[currentKey] = existingVal + op.Value
			kv.clientLastSeqNum[op.ClerkId] = op.RequestId
		}
	case "Get":
	default:
		fmt.Printf("Unknown operation: %s\n", op.Operation) // Or log the error
	}

	if ch, ok := kv.waitCh[uniqueRequestId]; ok {
		ch <- kv.inMem[currentKey]
		close(ch)
		delete(kv.waitCh, uniqueRequestId)
	}

}

func (kv *KVServer) recover(data []byte) {
	if data == nil || len(data) < 1 {
		data = kv.persister.ReadSnapshot()
	}

	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvData map[string]string
	var seenIds map[int64]int64

	if d.Decode(&kvData) != nil || d.Decode(&seenIds) != nil {
		log.Fatal("kv recover err")
	} else {
		kv.inMem = kvData
		kv.clientLastSeqNum = seenIds
	}
}

func (kv *KVServer) encodeKVData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.inMem); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.clientLastSeqNum); err != nil {
		panic(err)
	}
	data := w.Bytes()
	return data
}

// StartKVServer servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.killCh = make(chan struct{})
	kv.inMem = make(map[string]string)
	kv.clientLastSeqNum = make(map[int64]int64)

	kv.waitCh = make(map[string]chan string)
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.recover(nil)
	go kv.readApplyCh()

	return kv
}

// Utils
