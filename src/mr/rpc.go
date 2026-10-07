package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// request by the worker must me empty because the coordinator will assign a task to the worker
type TaskRequestArgs struct {
}

type TaskReply struct {
	// id needed for lab's naming convention
	ID          int
	InputFile   string
	TaskType    TaskType
	MapCount    int
	ReduceCount int
	ReplyState  ReplyState
}

type ReplyState int

const (
	TaskAvailable ReplyState = iota
	NoTaskAvailable
	AllTasksFinished
)

type TaskFinishedArgs struct {
	ID       int
	TaskType TaskType
}

type TaskFinishedReply struct {
}

// Add your RPC definitions here.
