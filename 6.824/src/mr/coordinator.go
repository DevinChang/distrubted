package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
	// map files
	mapFiles []string
	// number of map worker and reduce worker
	mapWorkers int `json:"-"`
	reduceWorkers int `json:"-"`
	// map worker
	mapWorkerDone []bool `json:"-"`
	// 用time.time可以用来进行时间上的控制
	mapWorkerRest []time.Time `json:"-"`
	// reduce worker
	reduceWorkerDone []bool `json:"-"`
	// 用time.time可以用来进行时间上的控制
	reduceWorkerRest []time.Time `json:"-"`
	// 结束标志
	shutdown bool `json:"-"`
	// mutex
	mutex *sync.Mutex
	// condition
	cond sync.Cond
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

// 调度worker
func (c *Coordinator) HandleAssignTask(req *GetTaskArg, reply *GetTaskResp) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// 返回有多少map worker和reduce worker，用于临时文件的生成
	reply.ReduceIntermediaTasks = c.reduceWorkers
	reply.WriteMapTask = c.mapWorkers
	// 先分配map work
	for {
		mapDone := true
		// 遍历所有worker，给已完成任务的worker分配任务
		for m, done := range c.mapWorkerDone {
			//
			if !done {
				if c.mapWorkerRest[m].IsZero() || time.Since(c.mapWorkerRest[m]).Seconds() > 10 {
					reply.TaskType = MapTask
					reply.TaskId = m
					reply.MapFile = c.mapFiles[m]
					c.mapWorkerRest[m] = time.Now()
					return nil
				}
			}

		}
		// 当所有任务都未完成，则等待, 否则执行reduce worker
		if !mapDone {
			// wait
			c.cond.Wait()
		} else {
			break
		}
	}
	// 再分配reduce work
	for {
		reduceDone := true
		// 遍历所有worker，给已完成任务的worker分配任务
		for r, done := range c.reduceWorkerDone {
			if !done {
				if c.reduceWorkerRest[r].IsZero() || time.Since(c.reduceWorkerRest[r]).Seconds() > 10 {
					reply.TaskType = ReduceTask
					reply.TaskId = r
					c.reduceWorkerRest[r] = time.Now()
				} else {
					reduceDone = false
				}
			}

		}
		// 当所有任务都未完成，则等待, 否则执行map worker
		if !reduceDone {
			// wait
			c.cond.Wait()
		} else {
			break
		}
	}
	// 所有任务执行完成
	reply.TaskType = DoneTask
	c.shutdown = true
	return
}

// 任务结束
func (c *Coordinator) HandleFinishedTask(req *TaskFinishArg, reply *TaskFinishedResp) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	switch req.TaskType {
	case MapTask :
		c.mapWorkerDone[req.TaskId] = true
	case ReduceTask:
		c.reduceWorkerDone[req.TaskId] = true
	default:
		DLog("Invalid TaskType:%d", req.TaskType)
	}
	c.cond.Broadcast()
	return
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mWorkers := len(files)
	c := Coordinator{
		mapWorkers: mWorkers,
		reduceWorkers: nReduce,
		mapWorkerDone: make([]bool, mWorkers),
		mapWorkerRest: make([]time.Time, mWorkers),
		reduceWorkerDone: make([]bool, nReduce),
		reduceWorkerRest: make([]time.Time, nReduce),
	}
	go func() {
		for {
			c.mutex.Lock()
			c.cond.Broadcast()
			c.mutex.Unlock()
			time.Sleep(time.Second)
		}
	}
	c.server()
	return &c
}
