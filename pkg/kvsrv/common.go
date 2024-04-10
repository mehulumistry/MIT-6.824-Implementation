package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string
	RequestId int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key       string
	RequestId int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
