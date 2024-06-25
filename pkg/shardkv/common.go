package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrWrongConfig     = "ErrWrongConfig" // Migrated
	ErrTimeOut         = "ErrTimeOut"     // Migrated
	ErrAlreadyMigrated = "ErrAlreadyMigrated"
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	Op        string
	RequestId int64
	ClerkId   int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err string
}

type Reply struct {
	Value   string
	Success bool
	Err     string
}

type GetArgs struct {
	Key       string
	RequestId int64
	ClerkId   int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   string
	Value string
}

type MigrateShardArgs struct {
	RequestId int64
	ClerkId   int64
	GID       int

	Shard            int               // The ID of the shard to migrate
	Data             map[string]string // The key-value pairs within the shard
	ConfigNum        int               // The configuration number for which the shard is being migrated
	ClientLastSeqNum map[int64]int64
}

type MigrateShardReply struct {
	Err string
}
