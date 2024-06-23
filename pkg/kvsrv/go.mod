module github.com/mehulumistry/MIT-6.824-Implementation/pkg/kvsrv

go 1.17

replace github.com/mehulumistry/MIT-6.824-Implementation/pkg/labrpc => ../labrpc

replace github.com/mehulumistry/MIT-6.824-Implementation/pkg/labgob => ../labgob

replace github.com/mehulumistry/MIT-6.824-Implementation/pkg/raft => ../raft

replace github.com/mehulumistry/MIT-6.824-Implementation/pkg/models => ../models

replace github.com/mehulumistry/MIT-6.824-Implementation/pkg/porcupine => ../porcupine

require (
	github.com/mehulumistry/MIT-6.824-Implementation/pkg/labrpc v0.0.0-20240512065008-bfe255afa50e
	github.com/mehulumistry/MIT-6.824-Implementation/pkg/models v0.0.0-20240407082225-fea525912f86
	github.com/mehulumistry/MIT-6.824-Implementation/pkg/porcupine v0.0.0-20240407082225-fea525912f86
)

require (
	github.com/DistributedClocks/GoVector v0.0.0-20240117185643-ae07272d0ebd // indirect
	github.com/daviddengcn/go-colortext v1.0.0 // indirect
	github.com/mehulumistry/MIT-6.824-Implementation/pkg/labgob v0.0.0-20240512065008-bfe255afa50e // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
)
