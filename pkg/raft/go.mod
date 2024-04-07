module github.com/mehulumistry/MIT-6.824-Implementation/pkg/raft

go 1.17

replace github.com/mehulumistry/MIT-6.824-Implementation/pkg/labgob => ../labgob

replace github.com/mehulumistry/MIT-6.824-Implementation/pkg/labrpc => ../labrpc

require (
	github.com/mehulumistry/MIT-6.824-Implementation/pkg/labgob v0.0.0-20220710125105-a6c914671086
	github.com/mehulumistry/MIT-6.824-Implementation/pkg/labrpc v0.0.0-00010101000000-000000000000
)
