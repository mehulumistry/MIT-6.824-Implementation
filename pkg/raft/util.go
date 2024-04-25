package raft

import (
	"log"
	"strconv"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func DPrintfId(id int, role string, format string, a ...interface{}) (n int, err error) {
	if Debug {
		strId := time.Now().String() + " SERVER_ID:" + "[" + strconv.Itoa(id) + "][" + role + "]"
		log.Printf(strId+format, a...)
	}
	return
}
