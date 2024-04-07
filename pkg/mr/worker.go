package mr

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)
import "net/rpc"
import "hash/fnv"

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

func getFileContent(filename string) string {
	dat, err := os.ReadFile(filename)
	HandleError(err)
	return string(dat)
}

// use getReducePartitionNumber(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func getReducePartitionNumber(key string, nReduce int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()&0x7fffffff) % nReduce
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerId := getWorkerId()
	EnableLogging(false)

	for {
		args := WorkerGetTaskRequest{WorkerId: workerId}
		reply := WorkerGetTaskReply{}
		MRPrintln("Worker: %v, GetTask\n", workerId)

		success := call("Coordinator.GetTask", &args, &reply)

		allProcessingDone := !success

		if allProcessingDone || reply.KillSignal {
			//MRPrintln("Worker: %v, Exiting...\n", workerId)
			break
		}

		if reply.EmptyQueue {
			//MRPrintln("No tasks found.....Sleeping.......\n")
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if success {
			MRPrintln("Worker: %v, taskId: %v, type: %v\n", workerId, reply.TaskId, reply.TaskType)

			switch reply.TaskType {
			case REDUCE:
				reduceWorkerExecute(workerId, reply, reducef)
			case MAP:
				mapWorkerExecute(workerId, reply, mapf)
			default:
				MRPrintln("Invalid task")

			}
		}

		time.Sleep(100 * time.Millisecond)
	}

}

func reduceWorkerExecute(
	workerId int,
	getTaskReply WorkerGetTaskReply,
	reducef func(string, []string) string,
) {
	// 1. Read content line by line

	// Combined grouping
	groupedValues := make(map[string][]string)

	// Process each file
	for _, fileName := range getTaskReply.FileLocs {
		content, _ := os.ReadFile(fileName)
		lines := strings.Split(string(content), "\n")

		keyValues := make([]KeyValue, 0, len(lines)) // Reset for each file
		for _, line := range lines {
			if line == "" {
				continue
			} // Skip empty lines
			parts := strings.Split(line, ",")
			if len(parts) == 2 {
				keyValues = append(keyValues, KeyValue{Key: parts[0], Value: parts[1]})
			} else {
				// Handle invalid lines (optional)
			}
		}

		// grouping
		for _, kv := range keyValues {
			groupedValues[kv.Key] = append(groupedValues[kv.Key], kv.Value)
		}
	}

	// Sort keys based on alphabets.
	keys := make([]string, 0, len(groupedValues))
	for key := range groupedValues {
		keys = append(keys, key)
	}

	// Sort alphabetically
	sort.Strings(keys)

	// Generate file name
	outputFileName := fmt.Sprintf("mr-out-%d", getTaskReply.TaskId)

	// Write to file
	fileContentBuilder := strings.Builder{}
	for _, key := range keys { // Iterate over the sorted keys
		values := groupedValues[key]
		valConcat := reducef(key, values)
		fileContentBuilder.WriteString(fmt.Sprintf("%v %v\n", key, valConcat))
	}

	tempFileName := fmt.Sprintf("mr-reduce-temp-%d", getTaskReply.TaskId)
	tempFile, err := os.Create(tempFileName)

	defer tempFile.Close()

	err = os.WriteFile(tempFileName, []byte(fileContentBuilder.String()), 0644)
	err = os.Rename(tempFileName, outputFileName)
	if err != nil {
		// Handle rename error
		return
	}

	updateCoordinatorWithCompletedTaskStatus(workerId, getTaskReply.TaskId, []string{})
}

func mapWorkerExecute(workerId int, taskReply WorkerGetTaskReply, mapf func(string, string) []KeyValue) {
	// Read file contents
	fileName := taskReply.FileLocs[0]
	fileContent := getFileContent(fileName)

	// Process with map function
	keyValuePairs := mapf(fileName, fileContent)

	// Map to store file contents (key: fileName, value: accumulated content)
	fileContentMap := make(map[string]string)
	tempFinalFileMap := make(map[string]string)
	fileLocs := make([]string, 0, len(fileContentMap))

	// Generate file names and update content map
	for _, kv := range keyValuePairs {
		reduceId := getReducePartitionNumber(kv.Key, taskReply.NReduce)
		outputFileName := fmt.Sprintf("mr-%d-%d", taskReply.TaskId, reduceId)
		// Accumulate content
		fileContentMap[outputFileName] += fmt.Sprintf("%v,%v\n", kv.Key, kv.Value)

		tempFileName := fmt.Sprintf("mr-temp-%d-%d", taskReply.TaskId, reduceId)
		tempFinalFileMap[outputFileName] = tempFileName
	}

	// Write to disk
	for finalFileName, content := range fileContentMap {
		// Saving as temp file
		tempFileName := tempFinalFileMap[finalFileName]
		file, _ := os.Create(tempFileName)

		err := os.WriteFile(tempFileName, []byte(content), 0644)
		if err != nil {
			// Handle potential write errors
			MRPrintln("Error writing to file %s: %v\n", fileName, err)
		}
		fileLocs = append(fileLocs, finalFileName)

		file.Close()
	}

	// Renaming to original file
	for finalFileName, tempFileName := range tempFinalFileMap {
		err := os.Rename(tempFileName, finalFileName)
		if err != nil {
			return
		}
	}

	updateCoordinatorWithCompletedTaskStatus(workerId, taskReply.TaskId, fileLocs)
}

func getWorkerId() int {
	args := WorkerRegisterRequest{}
	reply := WorkerRegisterReply{}
	call("Coordinator.GetWorkerId", &args, &reply)
	return reply.WorkerId
}

func updateCoordinatorWithCompletedTaskStatus(
	workerId int,
	taskId int,
	newFileLocs []string,
) {
	args := WorkerUpdateTaskStatusRequest{
		WorkerId:          workerId,
		TaskId:            taskId,
		NewGeneratedFiles: newFileLocs,
		TStatus:           COMPLETED,
	}
	reply := WorkerUpdateTaskStatusReply{}
	call("Coordinator.UpdateTaskStatus", &args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		MRPrintln("Coordinator disconnected, finished:")
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	return false
}
