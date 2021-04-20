package mr

import (
	"fmt"
	"log"
	"os"
)

func DLog(format string, a ...interface{}) (n int, err error) {
	log.Printf(format, a...)
	return
}


func interName(mWorker, rWorker int) string {
	return fmt.Sprintf("mr-%d-%d", mWorker, rWorker)
}

func reName(tmpFile string, mWorker, rWorker int) {
	key := interName(mWorker, rWorker)
	os.Rename(tmpFile, key)
}

func getIntermediateFile(mWorker, rWorker int) string {
	return fmt.Sprintf("mr-%d-%d", mWorker, rWorker)
}

func renameReduceFile(rName string, rWorker int) {
	reduceFileName := fmt.Sprintf("mr-out-%d", rWorker)
	os.Rename(rName, reduceFileName)
}