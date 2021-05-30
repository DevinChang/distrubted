package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (b ByKey) Len() int {
	return len(b)
}

func (b ByKey) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b ByKey) Less(i, j int) bool {
	return b[i].Key < b[j].Key
}


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func mapTask(filename string, taskNum int, nReduceTasks int, mapf func(string,string)[]KeyValue) {
	// read file
	file, err := os.Open(filename)
	if err != nil {
		DLog("open file(%s) error", filename)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		DLog("read file(%s) error", filename)
		return
	}
	file.Close()
	// exec main map logic
	kv := mapf(filename, string(content))
	// create temporary file
	tmpFiles := []*os.File{}
	tmpFileNames := []string{}
	encoder := []*json.Encoder{}
	for r := 0; r < nReduceTasks; r++ {
		tmpFile, err := ioutil.TempFile("","")
		if err != nil {
			DLog("can not create tmpfile")
			return
		}
		tmpFiles = append(tmpFiles, tmpFile)
		tmpFileNames = append(tmpFileNames, tmpFile.Name())
		enc := json.NewEncoder(tmpFile)
		encoder = append(encoder, enc)
	}
	// 将map的文件写入到临时文件中去
	for _, item := range kv {
		// 有n个reduce worker，就需要取模
		r := ihash(item.Key) % nReduceTasks
		encoder[r].Encode(&item)
	}
	// 关闭临时文件
	for _, tmp := range tmpFiles {
		tmp.Close()
	}
	// rename
	for r := 0; r < nReduceTasks; r++ {
		finalizeIntermediateFile(tmpFileNames[r], taskNum, r)
	}
}

func reduceTask(taskNum int, nMapTasks int, reducef func(string, []string) string) {
	// 获取所有kv
	kvs := []KeyValue{}
	for m := 0; m < nMapTasks; m++ {
		rFileName := getIntermediateFile(m, taskNum)
		rfile, err := os.Open(rFileName)
		if err != nil {
			log.Fatal("Open reduce File error")
		}
		dec := json.NewDecoder(rfile)
		for {
			var kv KeyValue
			if err = dec.Decode(&kv); err != nil {
				log.Printf("Reduce task decode kv(%v) error(%v)", kv, err)
				break
			}
			kvs = append(kvs, kv)
		}
		rfile.Close()
	}
	// reduce key
	sort.Sort(ByKey(kvs))
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatal("Reduce Open File error")
		return
	}
	tmpFileName := tmpFile.Name()
	keyStart := 0
	for keyStart < len(kvs) {
		keyEnd := keyStart + 1
		for keyEnd < len(kvs) && kvs[keyEnd].Key == kvs[keyStart].Key {
			keyEnd++
		}
		values := []string{}
		for k := keyStart; k < keyEnd; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[keyStart].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpFile, "%v %v\n", kvs[keyStart].Key, output)
		keyStart = keyEnd
	}
	finalizeReduceFile(tmpFileName, taskNum)
	//tmpFile.Close()
}




//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// 死循环用来不断接收任务
	for {
		taskArg := GetTaskArg{}
		taskReply := GetTaskResp{}
		call("Coordinator.HandleAssignTask", &taskArg, &taskReply)
		//log.Printf("Worker Get TaskID(%d) TaskType(%v)", taskReply.TaskId, taskReply.TaskType)
		switch taskReply.TaskType {
		case MapTask:
			//log.Printf("Woker Assign Map Task")
			mapTask(taskReply.MapFile, taskReply.TaskId, taskReply.NReduceTasks, mapf)
		case ReduceTask:
			//log.Printf("Woker Assign Reduce Task")
			reduceTask(taskReply.TaskId, taskReply.NMapTasks, reducef)
		case DoneTask:
			os.Exit(0)
		default:
			fmt.Errorf("Bad TaskType: %v", taskReply.TaskType)
		}// 任务完成
		finishArg := TaskFinishArg{
			TaskType: taskReply.TaskType,
			TaskId: taskReply.TaskId,
		}
		finishReply := TaskFinishedResp{}
		call("Coordinator.HandleFinishedTask", &finishArg, &finishReply)
	}


}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
