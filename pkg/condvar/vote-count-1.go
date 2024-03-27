package main

import "time"
import "math/rand"

func main() {

	randVal := time.Now().UnixNano()
	println(randVal)

	rand.New(rand.NewSource(randVal))

	sharedRandomCount := 0
	sharedFinished := 0

	for i := 0; i < 10; i++ {
		go func() {
			vote := requestVote()
			if vote {
				sharedRandomCount++
			}

			sharedFinished++
		}()
	}

	/*
		   Problems:
				1) Updating shared variable count and finished from different threads?
				2) sharedFinished gets stuck at 9? why?
				3) If I remove the requestVote() it's working fine, sharedFinished is always 10. why? adding requestVote() is making it like this

			go run --race file_name.go

			Solution:
				Use locks

	*/

	// while loop
	for sharedRandomCount < 5 && sharedFinished != 10 {
		// wait
		//println("count < 5 && finished != 10")
	}

	if sharedFinished == 10 {
		println("All go routines are done!")
	} else {
		println("SharedFinished: ", sharedFinished)
	}

	if sharedRandomCount >= 5 {
		println("received 5+ votes!")
	} else {
		println("lost")
	}
}

func requestVote() bool {
	// Sleeps randomly
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	// Generates rand bool
	return rand.Int()%2 == 0
}
