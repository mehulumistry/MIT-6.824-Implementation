module github.com/mehulumistry/MIT-6.824-Implementation/pkg/shardkv-2015

go 1.17

replace github.com/mehulumistry/MIT-6.824-Implementation/pkg/shardmaster => ../shardmaster

replace github.com/mehulumistry/MIT-6.824-Implementation/pkg/paxos => ../paxos

require (
	github.com/mehulumistry/MIT-6.824-Implementation/pkg/paxos v0.0.0-00010101000000-000000000000
	github.com/mehulumistry/MIT-6.824-Implementation/pkg/shardmaster v0.0.0-00010101000000-000000000000
)
