module github.com/arindas/mit-6.824-distributed-systems/pkg/shardctrler

go 1.22

replace github.com/arindas/mit-6.824-distributed-systems/pkg/labrpc => ../labrpc

replace github.com/arindas/mit-6.824-distributed-systems/pkg/labgob => ../labgob

replace github.com/arindas/mit-6.824-distributed-systems/pkg/raft => ../raft

replace github.com/arindas/mit-6.824-distributed-systems/pkg/models => ../models

replace github.com/arindas/mit-6.824-distributed-systems/pkg/porcupine => ../porcupine

require (
	github.com/arindas/mit-6.824-distributed-systems/pkg/labgob v0.0.0-20220710125105-a6c914671086
	github.com/arindas/mit-6.824-distributed-systems/pkg/labrpc v0.0.0-00010101000000-000000000000
	github.com/arindas/mit-6.824-distributed-systems/pkg/raft v0.0.0-00010101000000-000000000000
)
