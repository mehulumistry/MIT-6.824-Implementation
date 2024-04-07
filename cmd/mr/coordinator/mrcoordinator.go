package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
// go run /Users/mehulmistry/Desktop/main/coding/personal_coding/DistributedSystems/cmd/mr/coordinator/mrcoordinator.go ../../../datasets/project-gutenberg/pg*.txt
// Please do not change this file.
//

import (
	"fmt"
	"os"
	"time"

	"github.com/arindas/mit-6.824-distributed-systems/pkg/mr"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}
	//startTime := time.Now() // Record the start time

	m := mr.MakeCoordinator(os.Args[1:], 10)

	for m.Done() == false {
		time.Sleep(time.Second)
	}

	//endTime := time.Now() // Record the end time
	//elapsedTime := endTime.Sub(startTime)
	//
	//fmt.Printf("Total time for completion: %v\n", elapsedTime)

	time.Sleep(time.Second)
}
