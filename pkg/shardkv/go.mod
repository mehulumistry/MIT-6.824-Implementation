module github.com/mehulumistry/MIT-6.824-Implementation/pkg/shardkv

go 1.17

replace github.com/mehulumistry/MIT-6.824-Implementation/pkg/labrpc => ../labrpc

replace github.com/mehulumistry/MIT-6.824-Implementation/pkg/labgob => ../labgob

replace github.com/mehulumistry/MIT-6.824-Implementation/pkg/raft => ../raft

replace github.com/mehulumistry/MIT-6.824-Implementation/pkg/models => ../models

replace github.com/mehulumistry/MIT-6.824-Implementation/pkg/porcupine => ../porcupine

replace github.com/mehulumistry/MIT-6.824-Implementation/pkg/shardctrler => ../shardctrler

require (
	github.com/mehulumistry/MIT-6.824-Implementation/pkg/labgob v0.0.0-20220710125105-a6c914671086
	github.com/mehulumistry/MIT-6.824-Implementation/pkg/labrpc v0.0.0-00010101000000-000000000000
	github.com/mehulumistry/MIT-6.824-Implementation/pkg/models v0.0.0-00010101000000-000000000000
	github.com/mehulumistry/MIT-6.824-Implementation/pkg/porcupine v0.0.0-00010101000000-000000000000
	github.com/mehulumistry/MIT-6.824-Implementation/pkg/raft v0.0.0-00010101000000-000000000000
	github.com/mehulumistry/MIT-6.824-Implementation/pkg/shardctrler v0.0.0-00010101000000-000000000000
)
