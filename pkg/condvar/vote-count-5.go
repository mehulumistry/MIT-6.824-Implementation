package main

import "time"
import "math/rand"

func main() {
	rand.Seed(time.Now().UnixNano())

	count := 0
	finished := 0

	// unbuffered queue you can assume them as a single capacity queue.
	ch := make(chan bool)

	for i := 0; i < 10; i++ {

		println("iter: ", i)

		go func() {
			ch <- requestVote()
		}()
	}

	/*
	  We don't need locks on count and finished now.

	*/

	for count < 5 && finished < 10 {
		v := <-ch
		if v {
			count += 1
		}
		finished += 1
	}

	if count >= 5 {
		println("received 5+ votes!")

	} else {
		println("lost")
	}
}

func requestVote() bool {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	return rand.Int()%2 == 0
}
