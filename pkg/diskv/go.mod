module github.com/mehulumistry/MIT-6.824-Implementation/pkg/diskv

go 1.17

replace github.com/mehulumistry/MIT-6.824-Implementation/pkg/paxos => ../paxos

replace github.com/mehulumistry/MIT-6.824-Implementation/pkg/shardmaster => ../shardmaster

require (
	github.com/mehulumistry/MIT-6.824-Implementation/pkg/paxos v0.0.0-20240407083918-ea2dca331521
	github.com/mehulumistry/MIT-6.824-Implementation/pkg/shardmaster v0.0.0-20240407083918-ea2dca331521
)
