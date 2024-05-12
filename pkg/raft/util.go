package raft

import (
	"fmt"
	"github.com/DistributedClocks/GoVector/govec"
	"strconv"
	"time"
)

// Debugging

const Debug = false

type LoggingUtils struct {
	debug  bool
	logger *govec.GoLog
}

func (utils *LoggingUtils) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		msg := fmt.Sprintf(format, a...)
		//utils.logger.LogLocalEvent(msg, govec.GetDefaultLogOptions())
		fmt.Println(time.Now().String() + "  " + msg)
	}
	return
}

func (utils *LoggingUtils) DPrintfId(requestId string, id int, role string, format string, a ...interface{}) (n int, err error) {
	if Debug {
		strId := time.Now().String() + " [SERVER_ID:" + "[" + strconv.Itoa(id) + "][" + role + "][RequestId:" + requestId + "]"
		msg := fmt.Sprintf(strId+format, a...)
		//utils.logger.LogLocalEvent(msg, govec.GetDefaultLogOptions())
		fmt.Println(msg)
	}
	return
}
