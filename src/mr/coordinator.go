package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mapTasks    []MapTask
	reduceTasks []ReduceTask
	phase       CoordinatorPhase

	mu sync.Mutex
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

// Needed in rpc communcation to identify the type of task being requested by coordinator.
type TaskType int

const (
	MapTaskType TaskType = iota
	ReduceTaskType
)

type CoordinatorPhase int

const (
	MapPhase CoordinatorPhase = iota
	ReducePhase
	FinishedPhase
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

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
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.phase == FinishedPhase
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		phase: MapPhase,
	}

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

func (c *Coordinator) AssignTask(args *TaskRequestArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.phase {
	case MapPhase:
		c.AssignMapTask(reply)
	case ReducePhase:
		c.AssignReduceTask(reply)
	case FinishedPhase:
		reply.ReplyState = AllTasksFinished
	}

	return nil
}

func (c *Coordinator) AssignMapTask(reply *TaskReply) {
	task := c.findMapTask()

	if task != nil {
		task.state = Started
		task.startedAt = time.Now()

		reply.ID = task.id
		reply.InputFile = task.inputFile
		reply.ReplyState = TaskAvailable
		reply.ReduceCount = len(c.reduceTasks)

		reply.TaskType = MapTaskType
		return
	}

	reply.ReplyState = NoTaskAvailable
}

// Helper function to find if a map task is available
func (c *Coordinator) findMapTask() *MapTask {
	// Check if any task is waiting
	for i := 0; i < len(c.mapTasks); i++ {
		if c.mapTasks[i].state == Waiting {
			return &c.mapTasks[i]
		}
	}

	// Check if any task has timed out
	for i := 0; i < len(c.mapTasks); i++ {
		if c.mapTasks[i].state == Started &&
			time.Since(c.mapTasks[i].startedAt) > 10*time.Second {
			return &c.mapTasks[i]
		}
	}

	return nil
}

// Helper function to check if all map tasks are finished
func (c *Coordinator) isAllMapTasksFinished() bool {
	for i := 0; i < len(c.mapTasks); i++ {
		if c.mapTasks[i].state != Finished {
			return false
		}
	}
	return true
}

func (c *Coordinator) AssignReduceTask(reply *TaskReply) {
	task := c.findReduceTask()

	if task != nil {
		task.state = ReduceStarted
		task.startedAt = time.Now()

		reply.ID = task.id
		reply.TaskType = ReduceTaskType
		reply.MapCount = len(c.mapTasks)
		reply.ReplyState = TaskAvailable

		return
	}

	reply.ReplyState = NoTaskAvailable
}

func (c *Coordinator) startReducePhase() {
	c.phase = ReducePhase

	for i := 0; i < len(c.reduceTasks); i++ {
		c.reduceTasks[i].state = ReduceWaiting
	}
}

func (c *Coordinator) findReduceTask() *ReduceTask {
	// First check if a Reduce task is waiting
	for i := 0; i < len(c.reduceTasks); i++ {
		if c.reduceTasks[i].state == ReduceWaiting {
			return &c.reduceTasks[i]
		}
	}

	// Check if any Reduce task has timed out
	for i := 0; i < len(c.reduceTasks); i++ {
		if c.reduceTasks[i].state == ReduceStarted &&
			time.Since(c.reduceTasks[i].startedAt) > 10*time.Second {
			return &c.reduceTasks[i]
		}
	}

	return nil
}

func (c *Coordinator) isAllReduceTasksFinished() bool {
	for i := 0; i < len(c.reduceTasks); i++ {
		if c.reduceTasks[i].state != ReduceFinished {
			return false
		}
	}
	return true
}

func (c *Coordinator) ReportTaskFinished(
	args *TaskFinishedArgs,
	reply *TaskFinishedReply,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == MapTaskType {
		for i := 0; i < len(c.mapTasks); i++ {
			if c.mapTasks[i].id == args.ID {
				c.mapTasks[i].state = Finished
				break
			}
		}

		if c.isAllMapTasksFinished() {
			c.startReducePhase()
		}
	} else if args.TaskType == ReduceTaskType {
		for i := 0; i < len(c.reduceTasks); i++ {
			if c.reduceTasks[i].id == args.ID {
				c.reduceTasks[i].state = ReduceFinished
				break
			}
		}

		if c.isAllReduceTasksFinished() {
			c.phase = FinishedPhase
		}
	}

	return nil
}
