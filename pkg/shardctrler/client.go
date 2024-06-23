package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"fmt"
	"github.com/mehulumistry/MIT-6.824-Implementation/pkg/labrpc"
	"math/big"
	"reflect"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

var mu sync.Mutex
var errorCount int // for TestCapital
var checked map[reflect.Type]bool

type Clerk struct {
	servers []*labrpc.ClientEnd
	//currentLeader int32
	clerkId    int64
	nextSeqNum int64
}

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
	//ck.currentLeader = -1
	return ck
}

func checkValue(value interface{}) {
	checkType(reflect.TypeOf(value))
}

func checkType(t reflect.Type) {
	k := t.Kind()

	mu.Lock()
	// only complain once, and avoid recursion.
	if checked == nil {
		checked = map[reflect.Type]bool{}
	}
	if checked[t] {
		mu.Unlock()
		return
	}
	checked[t] = true
	mu.Unlock()

	switch k {
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			rune, _ := utf8.DecodeRuneInString(f.Name)
			if unicode.IsUpper(rune) == false {
				// ta da
				fmt.Printf("labgob error: lower-case field %v of %v in RPC or persist/snapshot will break your Raft\n",
					f.Name, t.Name())
				mu.Lock()
				errorCount += 1
				mu.Unlock()
			}
			checkType(f.Type)
		}
		return
	case reflect.Slice, reflect.Array, reflect.Ptr:
		checkType(t.Elem())
		return
	case reflect.Map:
		checkType(t.Elem())
		checkType(t.Key())
		return
	default:
		return
	}
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num:       num,
		RequestId: ck.nextSeqNum,
		ClerkId:   ck.clerkId,
	}

	ck.nextSeqNum++

	for {
		//leader := ck.currentLeader
		//
		//// Try the cached leader first
		//if leader != -1 {
		//	var reply QueryReply
		//	ok := ck.servers[leader].Call("ShardCtrler.Query", args, &reply)
		//	if ok && !reply.WrongLeader {
		//		return reply.Config
		//	} else {
		//		ck.currentLeader = -1 // Mark leader as unknown since this attempt failed
		//	}
		//}

		// If the leader is unknown or wrong, try all other servers
		for _, srv := range ck.servers {
			//if int32(i) != leader {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && !reply.WrongLeader && reply.Err != "Timeout" {
				//ck.currentLeader = int32(i)
				return reply.Config
			}

		}
		//ck.currentLeader = -1
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{Servers: servers, RequestId: ck.nextSeqNum, ClerkId: ck.clerkId}
	ck.nextSeqNum++ // Increment the sequence number
	for {
		//leader := ck.currentLeader
		//
		//// Try the cached leader first
		//if leader != -1 {
		//	var reply JoinReply
		//	ok := ck.servers[leader].Call("ShardCtrler.Join", args, &reply)
		//	if ok && !reply.WrongLeader && reply.Err != "Timeout" {
		//		return
		//	} else {
		//		ck.currentLeader = -1 // Mark leader as unknown since this attempt failed
		//	}
		//}

		// If the leader is unknown or wrong, try all other servers
		for _, srv := range ck.servers {
			//if int32(i) != leader {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && !reply.WrongLeader && reply.Err != "Timeout" {
				//ck.currentLeader = int32(i)
				return
			}

		}
		//ck.currentLeader = -1
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{GIDs: gids, RequestId: ck.nextSeqNum, ClerkId: ck.clerkId}
	ck.nextSeqNum++
	for {
		//leader := ck.currentLeader
		//
		//// Try the cached leader first
		//if leader != -1 {
		//	var reply LeaveReply
		//	ok := ck.servers[leader].Call("ShardCtrler.Leave", args, &reply)
		//	if ok && !reply.WrongLeader && reply.Err != "Timeout" {
		//		return
		//	} else {
		//		ck.currentLeader = -1 // Mark leader as unknown since this attempt failed
		//	}
		//}

		// If the leader is unknown or wrong, try all other servers
		for _, srv := range ck.servers {
			//if int32(i) != leader {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && !reply.WrongLeader && reply.Err != "Timeout" {
				//ck.currentLeader = int32(i)
				return
			}

		}
		//ck.currentLeader = -1
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{Shard: shard, GID: gid, RequestId: ck.nextSeqNum, ClerkId: ck.clerkId}
	ck.nextSeqNum++
	for {
		//leader := ck.currentLeader
		//
		//// Try the cached leader first
		//if leader != -1 {
		//	var reply MoveReply
		//	ok := ck.servers[leader].Call("ShardCtrler.Move", args, &reply)
		//	if ok && !reply.WrongLeader && reply.Err != "Timeout" {
		//		return
		//	} else {
		//		ck.currentLeader = -1 // Mark leader as unknown since this attempt failed
		//	}
		//}

		// If the leader is unknown or wrong, try all other servers
		for i, srv := range ck.servers {
			//if int32(i) != leader {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && !reply.WrongLeader && reply.Err != "Timeout" {
				//ck.currentLeader = int32(i)
				DPrintf("Setting the leader to : %d", i)
				return
			}

		}
		//ck.currentLeader = -1
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) randomizeLeaderId() int {
	max := big.NewInt(int64(len(ck.servers)))
	n, _ := rand.Int(rand.Reader, max)
	return int(n.Int64())
}
