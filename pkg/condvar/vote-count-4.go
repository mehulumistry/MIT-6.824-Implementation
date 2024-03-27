package main

import "sync"
import "time"
import "math/rand"

func main() {
	rand.Seed(time.Now().UnixNano())

	count := 0
	finished := 0
	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	for i := 0; i < 10; i++ {
		go func() {
			vote := requestVote()
			mu.Lock()
			defer mu.Unlock()
			if vote {
				count++
			}
			finished++
			// Broadcast is not blocking operation
			cond.Broadcast()
		}()
	}

	mu.Lock() // This is necessary because for the first time we want to acquire lock and check the variables.

	/**
	The mutex and the condition variable work together to synchronize the goroutines and
	the main thread, ensuring they don't step on each other's toes when accessing shared data.
	*/
	for count < 5 && finished != 10 {
		/*
			1) Releases the mutex
			2) Blocks the thread (goes to sleep)
			3) Reacquires the mutex upon waking up --> performs 3) Only after signal from broadcast
		*/
		cond.Wait()
	}
	if count >= 5 {
		println("received 5+ votes!")
	} else {
		println("lost")
	}
	mu.Unlock()
}

func requestVote() bool {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	return rand.Int()%2 == 0
}
