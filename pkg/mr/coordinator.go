package mr

import "C"
import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

/*

1 Coordinator
1 or more workers in parallel

worker <--RPC---> coordinator
workers in a loop asks for the tasks from coordinator

1) Read the task's input from one or more files,
2) execute the task,
3) write the task's output to one or more files

and again ask the coordinator for new task.


Cord --> if a worker hasn't completed the task in a reasonable amount of time (10 seconds) then give the task to another worker.

Don't change the structure of the files.

Run the sequential file and match the output file with your version of map reduce output. They should match.

test-mr.sh script --> this test checks if word count and indexer are working on your version of mr.
It checks that map and reduce tasks are running in parallel,
and it recovers from workers that crash while running tasks.

The test script expects to see output in files named mr-out-X, one for each reduce task

worker should put mr-out-X (reduce task output)

one line per reduce function in the file. "%v %v" format (key, value)

When the job is completely finished, the worker processes should exit.
A simple way to implement this is to use the return value from call(): if the worker fails to contact the coordinator,
it can assume that the coordinator has exited because the job is done, so the worker can terminate too. Depending on your design,
you might also find it helpful to have a "please exit" pseudo-task that the coordinator can give to workers.

Temp file output from Map can be mr-x-y (x is the map task number, y is the reduce task number)
mapper should create nReduce number of files for reduce tasks to process.

*/

type TaskType int
type TaskStatus int

type WorkerStatus int

const (
	MAP TaskType = iota
	REDUCE
)

const (
	IDLE TaskStatus = iota
	COMPLETED
	IN_PROGRESS
)

const (
	HEALTHY WorkerStatus = iota
	DEAD
	EXITED
)

type TaskPtr map[int]*Task

type Task struct {
	Id          int // M
	NReduce     int // fix const
	FileLocs    []string
	TaskType    TaskType
	TStatus     TaskStatus
	WorkerId    int
	WStatus     WorkerStatus
	ExpiryTimer *time.Timer
}

type Coordinator struct {
	idleTasks           TaskPtr
	completedTasks      TaskPtr
	inProgressTasks     TaskPtr
	workers             TaskPtr // healthy workers
	timeoutNotification chan *Task
	doneChan            chan bool
	done                bool
	init                bool
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	MRPrintln(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	if c.done {
		c.shutdown()
		MRPrintln("\n\n\n\n *******************************DONE*******************************\n\n\n\n")
	}
	return c.done
}

var nextID int = 0 // Monotonically increasing ID counter

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	EnableLogging(false)

	c := Coordinator{}
	c.init = true
	c.server()

	c.idleTasks = make(TaskPtr)
	c.completedTasks = make(TaskPtr)
	c.inProgressTasks = make(TaskPtr)
	c.workers = make(TaskPtr)
	c.done = false
	c.timeoutNotification = make(chan *Task)
	c.doneChan = make(chan bool)

	go c.manageTaskTimeouts()

	for i, fileName := range files {
		c.idleTasks[i] = &Task{
			Id:       i,
			NReduce:  nReduce,
			FileLocs: []string{fileName},
			TaskType: MAP,
			TStatus:  IDLE,
		}
	}

	c.init = false

	MRPrintln("\n\nTotal Splits: %v", len(files))
	MRPrintln("\n------------------- Starting MAP Phase -------------------\n")

	return &c
}

func (c *Coordinator) shutdown() {
	close(c.doneChan) // Close the channel to signal goroutine
}

var lock sync.Mutex

func (c *Coordinator) UpdateTaskStatus(
	args *WorkerUpdateTaskStatusRequest,
	reply *WorkerUpdateTaskStatusReply,
) error {
	// TODO: Try to optimize this lock
	lock.Lock()
	defer lock.Unlock()

	c.validateWorkerId(args.WorkerId)

	MRPrintln("WorkerId: %v, taskId: %v --- COMPLETED ---\n", args.WorkerId, args.TaskId)

	var foundItem bool
	var itemToMove Task

	switch args.TStatus {
	case COMPLETED:
		inProgressTask, found := c.inProgressTasks[args.TaskId]
		if found {
			foundItem = true
			itemToMove = *inProgressTask
			delete(c.inProgressTasks, args.TaskId)
		}

	default:
		return fmt.Errorf("unsupported task status: %v", args.TStatus)
	}

	if !foundItem {
		return fmt.Errorf("task %d not found for status update", args.TaskId)
	}

	switch args.TStatus {
	case COMPLETED:
		itemToMove.ExpiryTimer.Stop()
		itemToMove.TStatus = COMPLETED
		itemToMove.FileLocs = args.NewGeneratedFiles

		c.completedTasks[args.TaskId] = &itemToMove
		reply.StatusUpdated = true

		if len(c.inProgressTasks) == 0 && len(c.idleTasks) == 0 {

			if itemToMove.TaskType == MAP {
				MRPrintln("Generated %v files \n\n", len(c.completedTasks))
				MRPrintln("\n\n----------------MAP DONE----------------\n")
				c.moveTasksFromCompletedToIdle()
			}

			MRPrintln("\n-------------Starting REDUCE Phase-------------\n")

			if itemToMove.TaskType == REDUCE {
				MRPrintln("Generated %v files \n\n", len(c.completedTasks))
				MRPrintln("\n\n----------------REDUCE DONE----------------\n")
				c.done = true
				break
			}

		}
	default:
		return fmt.Errorf("unsupported task status: %v", itemToMove.TStatus)
	}

	return nil
}

func printTaskList(name string, taskList TaskPtr) {
	MRPrintln(name + ":")

	if len(taskList) == 0 {
		MRPrintln("  (No Tasks)")
		return
	}

	for taskId, taskPtr := range taskList {
		MRPrintln("  Task %d: Id: %d, file %s", taskId, taskPtr.Id, taskPtr.FileLocs)
	}
}

func (c *Coordinator) moveTasksFromCompletedToIdle() {
	re := regexp.MustCompile(`mr-\d+-(\d+)`)

	for taskId, task := range c.completedTasks {

		// TODO: O(N^2) loop you can optimize this.
		// Also, load is not distributed to many worker in case of reduce case.
		// If some keys with 0 partition get more keys then we are overloading one worker with too many keys.
		// Other workers should be able to steal work from others in such overloading cases.
		// Or divide the tasks in such a way that reducer gets less load.

		for _, fileLoc := range task.FileLocs {

			matches := re.FindStringSubmatch(fileLoc)
			if len(matches) > 1 {

				yValue, err := strconv.Atoi(matches[1])
				if err != nil {
					// Handle error converting "y" to an integer
				} else {
					// Create the entry in idleTasks if it doesn't exist
					if _, exists := c.idleTasks[yValue]; !exists {
						c.idleTasks[yValue] = &Task{
							Id:       yValue,
							TaskType: REDUCE,
							NReduce:  task.NReduce,
							TStatus:  IDLE,
						}
					}

					c.idleTasks[yValue].FileLocs = append(c.idleTasks[yValue].FileLocs, fileLoc)

				}
			}
		}

		delete(c.completedTasks, taskId)
	}

}
func (c *Coordinator) GetTask(
	args *WorkerGetTaskRequest,
	reply *WorkerGetTaskReply,
) error {
	lock.Lock() // Assuming you have a lock for thread-safety
	defer lock.Unlock()

	workerId := args.WorkerId
	validWorker := c.validateWorkerId(workerId)

	if !validWorker {
		reply.KillSignal = true
		return nil
	}

	for taskID, task := range c.idleTasks {
		delete(c.idleTasks, taskID) // Remove from idle

		// Tasks assignment
		task.WorkerId = workerId
		task.TStatus = IN_PROGRESS
		task.WStatus = HEALTHY

		task.ExpiryTimer = time.AfterFunc(10*time.Second, func() {
			c.timeoutNotification <- task
		})

		// Add to in-progress
		c.inProgressTasks[taskID] = task

		// Add to workers tracking list
		c.workers[workerId] = task

		reply.TaskId = task.Id
		reply.FileLocs = task.FileLocs
		reply.NReduce = task.NReduce
		reply.TaskType = task.TaskType
		reply.EmptyQueue = false
		return nil
	}
	reply.EmptyQueue = true
	return nil

}

func (c *Coordinator) manageTaskTimeouts() {
	for {
		select {
		case task := <-c.timeoutNotification: // Block and wait for events
			c.handleTaskTimeout(task)
		case <-c.doneChan: // Add a case for the 'done' channel
			MRPrintln("Task timeout manager exiting...")
			return // Terminate the goroutine
		}
	}
}

func (c *Coordinator) handleTaskTimeout(task *Task) {

	task.ExpiryTimer.Stop() // Prevent future triggers if not re-adding the task

	if task.TStatus == IN_PROGRESS {
		inProgressTask, found := c.inProgressTasks[task.Id]
		if found {
			delete(c.inProgressTasks, inProgressTask.Id)
		}

		c.workers[task.WorkerId].WStatus = DEAD
		task.TStatus = IDLE
		c.idleTasks[task.Id] = task

		MRPrintln("Worker Id: %v crashed, taskId: %v, moving the task to Idle\n",
			task.WorkerId, task.Id)
	}

}

func (c *Coordinator) validateWorkerId(workerId int) bool {
	task, exists := c.workers[workerId]
	if !exists {
		println("Something malicious, this worker doesn't exist.....")
		return true // Worker ID doesn't exist
	}

	// Check if worker is healthy.
	if task.WStatus == HEALTHY {
		return true
	}

	return false

}

// GetWorkerId Give monotonically increasing Ids.
func (c *Coordinator) GetWorkerId(
	args *WorkerRegisterRequest,
	reply *WorkerRegisterReply,
) error {

	lock.Lock()

	defer lock.Unlock()

	c.workers[nextID] = &Task{
		Id:       -1, // not assigned
		WorkerId: nextID,
		WStatus:  HEALTHY,
	}
	reply.WorkerId = nextID
	nextID++ // Increment for the next ID
	return nil
}
