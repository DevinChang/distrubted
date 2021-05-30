package mr

import (
	"fmt"
	"log"
	"os"
)

func DLog(format string, a ...interface{}) {
	log.Printf(format, a...)
	return
}

func finalizeReduceFile(tmpFile string, taskN int){
	finalFile := fmt.Sprintf("mr-out-%d", taskN)
	os.Rename(tmpFile, finalFile)
}

func getIntermediateFile(mapTaskN int, reduceTaskN int) string{
	return fmt.Sprintf("mr-%d-%d", mapTaskN, reduceTaskN)
}

func finalizeIntermediateFile(tmpFile string, mapTaskN int, reduceTaskN int){
	finalFile := getIntermediateFile(mapTaskN, reduceTaskN)
	os.Rename(tmpFile, finalFile)
}




