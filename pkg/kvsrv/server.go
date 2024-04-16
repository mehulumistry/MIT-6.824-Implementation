package kvsrv

import (
	"log"
	"strconv"
	"sync"
)

// Debug
//
// # In mem map of key-value pairs
//
// /**
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// Also, validate your excalidraw case, how porcupine is making sure that in concurrent cases, two cases 1, or 2 both
// possible and in case of 5 concurrent threads there can be more possibilities. How porupine is passing the test cases
// in those cases. Also, it's giving your errors in real time, then does it even checks the sequential vs your server. I doubt

type KVServer struct {
	mu      sync.Mutex
	inMem   map[string]string
	seenIds map[int64]uint64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, exists := kv.inMem[args.Key]
	if !exists {
		reply.Value = "" // Return empty string if key doesn't exist
	} else {
		reply.Value = value
	}

	DPrintf("Get Value of key: %v, %v, id: %v", args.Key, reply.Value, ShowLast4digit(args.RequestId))
}

func ShowLast4digit(val int64) string {
	str := strconv.FormatInt(val, 10)

	// Get the last 4 digits of the string
	return str[len(str)-4:]
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 1. Check for Duplicates
	if existingData, exists := kv.seenIds[args.RequestId]; exists {
		DPrintf("Duplicate Request...%v", ShowLast4digit(args.RequestId))
		endIndex := existingData
		reply.Value = kv.inMem[args.Key][0:endIndex]
		return
	}

	DPrintf("Putting value: %v, %v, id: %v, Op: %v", args.Key, args.Value, ShowLast4digit(args.RequestId), args.Op)

	// 2. Process the Request (existing logic)
	if args.Op == "Put" {
		kv.inMem[args.Key] = args.Value
		kv.seenIds[args.RequestId] = uint64(len(args.Value))
	} else if args.Op == "Append" {
		oldValue, exists := kv.inMem[args.Key]
		if !exists {
			oldValue = "" // Treat as empty if the key doesn't exist
		}
		kv.inMem[args.Key] = oldValue + args.Value
		reply.Value = oldValue
		kv.seenIds[args.RequestId] = uint64(len(oldValue))
	}

	DPrintf("InMem now value for key: %v, %v, Op: %v", args.Key, kv.inMem[args.Key], args.Op)

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.PutAppend(args, reply)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.PutAppend(args, reply)
}

func StartKVServer() *KVServer {
	kv := &KVServer{
		mu:      sync.Mutex{},            // Initialize the mutex
		inMem:   make(map[string]string), // Initialize the in-memory map
		seenIds: make(map[int64]uint64),  // Initialize the RequestId tracking map
	}
	return kv
}
