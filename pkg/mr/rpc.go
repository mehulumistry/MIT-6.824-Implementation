package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

type WorkerGetTaskRequest struct {
	WorkerId int
}

type WorkerGetTaskReply struct {
	EmptyQueue bool
	TaskId     int // M
	NReduce    int // fix const
	FileLocs   []string
	TaskType   TaskType
	KillSignal bool
}

type WorkerUpdateTaskStatusRequest struct {
	WorkerId          int
	TaskId            int // M
	NewGeneratedFiles []string
	TStatus           TaskStatus
}

type WorkerUpdateTaskStatusReply struct {
	StatusUpdated bool
}

type WorkerRegisterRequest struct{}

type WorkerRegisterReply struct {
	WorkerId int
}

//	type WorkerRequest struct {
//		WorkerId int
//		Task     Task
//	}
//
//	type WorkerReply struct {
//		Task Task
//	}
type CoordinatorRequest struct {
	WorkerId int
	TaskType TaskType
}
type CoordinatorReply struct {
	FileNames string
}

type RetryableError struct {
	msg string
}

func (e *RetryableError) Error() string {
	return e.msg
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
