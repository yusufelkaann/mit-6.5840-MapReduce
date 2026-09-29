package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mapTasks    []MapTask
	reduceTasks []ReduceTask
}

type MapTaskState int

const (
	Waiting MapTaskState = iota
	Started
	Finished
)

// This represents a single map task (an input file to be processed)
type MapTask struct {
	id        int
	state     MapTaskState
	inputFile string
	startedAt time.Time
}

type ReduceTaskState int

const (
	ReduceNotReady ReduceTaskState = iota
	ReduceWaiting
	ReduceStarted
	ReduceFinished
)

type ReduceTask struct {
	id        int
	state     ReduceTaskState
	startedAt time.Time
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *TaskRequestArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	for index, file := range files {
		c.mapTasks = append(c.mapTasks, MapTask{
			id:        index,
			state:     Waiting,
			inputFile: file,
		})
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, ReduceTask{
			id:    i,
			state: ReduceNotReady,
		})
	}

	c.server(sockname)
	return &c
}
