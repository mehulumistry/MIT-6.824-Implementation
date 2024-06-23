package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid (shard[3] --> 2) shard 3 is responsible for replica group 2.
	Groups map[int][]string // gid -> servers[]   replica group -> servers list.
	// Each replica group is responsible for subset of shards. Gets/Puts
	// Client finds which ReplicaGroupId from controller to connect for the specific key and replicaGroups call controller to find
	// which shards they are serving time to time.
	// What if put comes in and controller forwarded it to GID 3 and then when it goes to GID 3, GID 3 asks controller for config and it got changed
	// and that particular PUT now is not served by GID 3. What would happen to the PUT?
	// You need to check if PUT is before or after. And accordingly MOVE the contents.
	// RGs also talk among each other.

	// NOTE: We are not checking which server has more load and then reshuffling, we distribute just taking all the replica groups into account.
}

func (c *Config) Copy() Config {
	config := Config{
		Num:    c.Num,
		Shards: c.Shards,
		Groups: make(map[int][]string),
	}
	for gid, s := range c.Groups {
		config.Groups[gid] = append([]string{}, s...)
	}
	return config
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	RequestId int64
	ClerkId   int64

	Servers map[int][]string // new GID -> servers mappings
}
type LeaveArgs struct {
	RequestId int64
	ClerkId   int64

	GIDs []int
}
type MoveArgs struct {
	RequestId int64
	ClerkId   int64

	Shard int
	GID   int
}
type QueryArgs struct {
	RequestId int64
	ClerkId   int64

	Num int // desired config number
}

type Reply struct {
	Value    Config
	IsLeader bool
	Success  bool
	Timeout  bool
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}
type LeaveReply struct {
	WrongLeader bool
	Err         Err
}
type MoveReply struct {
	WrongLeader bool
	Err         Err
}
type QueryReply struct {
	WrongLeader bool
	Config      Config
	Err         Err
}
