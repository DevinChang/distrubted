package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskType int

const (
	MapTask = TaskType(1)
	ReduceTask = TaskType(2)
	DoneTask = TaskType(3)
)

// Add your RPC definitions here.
// 请求任务
type GetTaskArg struct {}

type GetTaskResp struct {
	TaskType TaskType
	TaskId int
	// map work需要读的文件
	MapFile string
	// map 需要写入的文件
	WriteMapTask int
	// reduce 读几个中间文件
	ReduceIntermediaTasks int
}

// 任务结束通知
type TaskFinishArg struct {
	// 任务类型
	TaskType TaskType
	TaskId int
}

type TaskFinishedResp struct {}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
