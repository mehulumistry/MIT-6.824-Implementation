module github.com/mehulumistry/MIT-6.824-Implementation/pkg/raft

go 1.17

replace github.com/mehulumistry/MIT-6.824-Implementation/pkg/labgob => ../labgob

replace github.com/mehulumistry/MIT-6.824-Implementation/pkg/labrpc => ../labrpc

require (
	github.com/mehulumistry/MIT-6.824-Implementation/pkg/labgob v0.0.0-20240430080521-f5af5ee7d58b
	github.com/mehulumistry/MIT-6.824-Implementation/pkg/labrpc v0.0.0-20240407082225-fea525912f86
)

require (
	github.com/DistributedClocks/GoVector v0.0.0-20240117185643-ae07272d0ebd // indirect
	github.com/daviddengcn/go-colortext v1.0.0 // indirect
	github.com/golang/mock v1.6.0 // indirect
	github.com/petermattis/goid v0.0.0-20180202154549-b0b1615b78e5 // indirect
	github.com/sasha-s/go-deadlock v0.3.1 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
)
