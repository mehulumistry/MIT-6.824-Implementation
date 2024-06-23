package kvraft

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string
	RequestId int64
	ClerkId   int64
}

type Reply struct {
	Value    string
	IsLeader bool
	Success  bool
	Timeout  bool
}

type GetArgs struct {
	Key       string
	RequestId int64
	ClerkId   int64
}
