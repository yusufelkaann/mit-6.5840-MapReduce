package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string

func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Our implementation will go here.

	for {
		args := TaskRequestArgs{}
		reply := TaskReply{}

		if !call("Coordinator.AssignTask", &args, &reply) {
			log.Fatalf("%d: RPC call failed", os.Getpid())
		}

		switch reply.ReplyState {
		case TaskAvailable:
			switch reply.TaskType {
			case MapTaskType:
				err := executeMapTask(reply, mapf)
				if err != nil {
					log.Fatalf("%d: failed to execute map task %d: %v", os.Getpid(), reply.ID, err)
					continue
				}

				ok := reportTaskFinished(reply.ID, MapTaskType)
				if !ok {
					return
				}
			case ReduceTaskType:
				// ---
			}

		case NoTaskAvailable:
			time.Sleep(500 * time.Millisecond)
			continue
		case AllTasksFinished:
			return
		}
	}
}

func executeMapTask(reply TaskReply, mapf func(string, string) []KeyValue) error {
	// read the input file
	content, err := os.ReadFile(reply.InputFile)
	if err != nil {
		log.Fatalf("%d: failed to read input file %s: %v", os.Getpid(), reply.InputFile, err)
	}

	// execute the map function
	kva := mapf(reply.InputFile, string(content))

	// Create intermediate files for each reduce task
	tempFiles := make([]*os.File, reply.ReduceCount)
	encoders := make([]*json.Encoder, reply.ReduceCount)

	for reduceId := 0; reduceId < reply.ReduceCount; reduceId++ {
		tempFile, err := os.CreateTemp("", "mr-tmp-*")
		if err != nil {
			log.Fatalf("%d: failed to create temp file for reduce task %d: %v", os.Getpid(), reduceId, err)
		}

		tempFiles[reduceId] = tempFile
		encoders[reduceId] = json.NewEncoder(tempFile)
	}

	// Partition each KeyValue into its Reduce
	for _, kv := range kva {
		reduceID := ihash(kv.Key) % reply.ReduceCount

		if err := encoders[reduceID].Encode(&kv); err != nil {
			log.Fatalf("%d: failed to encode KeyValue for reduce task %d: %v", os.Getpid(), reduceID, err)
		}
	}

	for reduceID, tempFile := range tempFiles {
		if err := tempFile.Close(); err != nil {
			return err
		}

		finalName := fmt.Sprintf("mr-%d-%d", reply.ID, reduceID)
		if err := os.Rename(tempFile.Name(), finalName); err != nil {
			return err
		}
	}

	return nil
}

func reportTaskFinished(id int, taskType TaskType) bool {
	args := TaskFinishedArgs{
		ID:       id,
		TaskType: taskType,
	}

	reply := TaskFinishedReply{}

	return call("Coorndinator.TaskFinished", &args, &reply)
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}

	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
