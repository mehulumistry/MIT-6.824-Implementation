package kvraft

import (
	"crypto/rand"
	"github.com/mehulumistry/MIT-6.824-Implementation/pkg/labrpc"
	"math/big"
	"sync/atomic"
	"time"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.randomizeLeaderId()
	ck.clerkId = nrand()
	ck.nextSeqNum = 0
	return ck
}

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
type Clerk struct {
	servers       []*labrpc.ClientEnd
	currentLeader int32
	clerkId       int64
	nextSeqNum    int64
}

func (ck *Clerk) callWithRetry(method string, args interface{}, reply *Reply) bool {
	n := len(ck.servers)
	startIndex := ck.randomizeLeaderId()
	offset := 0

	for {
		var i int

		leader := atomic.LoadInt32(&ck.currentLeader)
		if leader == -1 {
			i = (startIndex + offset) % n
			offset++
		} else {
			i = int(leader)
		}

		DPrintf("[REQUEST][Clerk: %d][%s][KVServer: %d][RequestId: %d][cmd: %+v]",
			ck.clerkId, method, i, getRequestId(args), args)

		ok := ck.servers[i].Call(method, args, reply)
		if ok {
			if reply.Success {
				atomic.StoreInt32(&ck.currentLeader, int32(i))
				DPrintf("[SUCCESS]Successfully executed %s requested..., [RequestId: %d][Clerk: %d][KVServer: %v][cmd: %+v]",
					method, getRequestId(args), ck.clerkId, ck.currentLeader, args)
				return true
			} else if !reply.IsLeader {
				DPrintf("[WRONG_LEADER][RETRY] %s requested..., [RequestId: %d][Clerk: %d][KVServer: %v][cmd: %+v]",
					method, getRequestId(args), ck.clerkId, ck.currentLeader, args)
			} else if reply.Timeout {
				DPrintf("[TIMEOUT][RETRY] %s requested..., [RequestId: %d][Clerk: %d][KVServer: %v][cmd: %+v]",
					method, getRequestId(args), ck.clerkId, ck.currentLeader, args)
			}
		} else {
			DPrintf("[NETWORK_FAILURE][RETRY] %s requested..., [RequestId: %d][Clerk: %d][KVServer: %v][cmd: %+v]",
				method, getRequestId(args), ck.clerkId, ck.currentLeader, args)
		}
		atomic.StoreInt32(&ck.currentLeader, -1)
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Get(key string) string {
	getArgs := GetArgs{
		Key:       key,
		RequestId: ck.nextSeqNum,
		ClerkId:   ck.clerkId,
	}
	reply := Reply{}
	ck.nextSeqNum += 1

	if ck.callWithRetry("KVServer.Get", &getArgs, &reply) {
		return reply.Value
	}
	return ""
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		RequestId: ck.nextSeqNum,
		ClerkId:   ck.clerkId,
	}
	reply := &Reply{}
	ck.nextSeqNum += 1

	ck.callWithRetry("KVServer.PutAppend", args, reply)
}

func getRequestId(args interface{}) int64 {
	switch v := args.(type) {
	case *GetArgs:
		return v.RequestId
	case *PutAppendArgs:
		return v.RequestId
	default:
		return 0
	}
}

func (ck *Clerk) randomizeLeaderId() int {
	max := big.NewInt(int64(len(ck.servers)))
	n, _ := rand.Int(rand.Reader, max)
	return int(n.Int64())
}
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
