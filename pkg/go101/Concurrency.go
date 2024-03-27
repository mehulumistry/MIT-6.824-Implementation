package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"
)

func main() {
	//
	//ch := make(chan string)
	//
	//go push("Mehul", ch)
	//go push("Alisha", ch)
	//go push("Family", ch)
	//
	//fmt.Println(<-ch, <-ch, <-ch)
	//
	//var wg sync.WaitGroup
	//wg.Add(1)
	//go starter(&wg)
	//
	//follow()
	//
	//wg.Wait()
	//
	//channels := make(chan string)
	//defer close(channels)
	//
	//go starterWithChannel(channels)
	//receiver := <-channels // passing msg from channel to receiver
	//
	//follower(receiver)

	// I'll fork new process, it will fork all the below three
	// It's fork-join model.
	//go someFunc("1")
	//go someFunc("1")
	//go someFunc("1")
	// When you run the above, the main will not wait for the above to finish.
	// functions newer rejoined main, so we can wait for the main and it will come back to main.

	fmt.Println("Hi")

	//forSelect()
	//forSelect2()
	//pipeline()
	// generate is only producing the value that we need; it's not inf generating values.
	//generateInfStreamOfData()
	//dataInconsistency()
	orDoneChannelFn()
}

// Pattern 1 for-select loop
func forSelect() {

	charChannel := make(chan string, 3)
	// Unbuffered is sync
	// with buffered its queue like functionality
	chars := []string{"a", "b", "c"}

	for _, s := range chars {
		select {
		case charChannel <- s:
		}
	}

	// we close the channel after putting everything in the channel
	// that's the buffered channel, so we can close it.
	close(charChannel)

	for result := range charChannel {
		fmt.Println(result)
	}

}

// Pattern 2 done channel
// We want to prevent the go routine leaking, preventing go to run forever if parent dies.
// Allowing parent go routine to cancel the child
func forSelect2() {

	// Infinite running go routine.
	// We want to cancel the below

	done := make(chan bool)

	go doWork(done)
	time.Sleep(time.Second * 3)

	// this will send the signal to the doWork
	close(done)

}

// Read only channel
// doWork function cannot write to this channel.

func doWork(done <-chan bool) {
	for {
		select {
		// if done is there than it will cancel.
		case <-done:
			return
		default:
			fmt.Println("Doing work....")
		}
	}
}

// Pattern 3 pipeline

func pipeline() {

	// input

	nums := []int{2, 3, 4, 7, 1}

	// stage 1

	// sliceToChannel and sq go routine are running at the same time
	// and are in sync.

	dataChannel := sliceToChannel(nums)

	// stage 2

	finalChannel := sq(dataChannel)

	// stage 3

	for n := range finalChannel {
		fmt.Println(n)
	}

}

func sq(in <-chan int) <-chan int {

	out := make(chan int)

	go func() {
		// range will wait, in will only have one value, and it blocks at range.
		for n := range in {
			out <- n * n
		}

		close(out)
	}()

	return out
}

// returns readonly channel
func sliceToChannel(nums []int) <-chan int {

	out := make(chan int)
	go func() {
		// it blocks, we are returning while it stills running.
		// sq is reading from the out channel.
		// sliceToChannel is putting value and then blocks
		// sq reads the value and then blocks, until there is another value.

		for _, n := range nums {
			out <- n
		}

		close(out)
	}()

	return out
}

// Pattern 4: Generators

// generating go routine.
func repeatFunc[T any, K any](done <-chan K, fn func() T) <-chan T {

	stream := make(chan T)

	go func() {
		defer close(stream)
		for {
			select {
			case <-done:
				return
			case stream <- fn():
			}
		}
	}()

	// even if you return, you are just returning the reference the go routine
	// will keep on adding values. Go routine will not get over

	return stream

}

// receiving go routine

func take[T any, K any](done <-chan K, stream <-chan T, n int) <-chan T {
	taken := make(chan T)
	go func() {
		defer close(taken)

		// only going to take n values and giving to the stream
		// this means once go routine doesn't want to receive from the channel
		// the routine in repeatFn will block.
		// after the loop the taken will be closed and it will return the take.
		for i := 0; i < n; i++ {
			select {
			case <-done:
				return
			case taken <- <-stream:
			}
		}

	}()

	return taken
}

func primeFinder(done <-chan int, randIntStream <-chan int) <-chan int {
	isPrime := func(randomInt int) bool {
		for i := randomInt - 1; i > 1; i-- {
			if randomInt%i == 0 {
				return false
			}
		}

		return true
	}

	primes := make(chan int)
	go func() {
		defer close(primes)

		for {
			select {
			case <-done:
				return
			case randInt := <-randIntStream:
				if isPrime(randInt) {
					primes <- randInt
				}
			}
		}

	}()

	return primes
}

func fanIn[T any](done <-chan int, channels ...<-chan T) <-chan T {
	var wg sync.WaitGroup
	fannedIntStream := make(chan T)

	transfer := func(c <-chan T) {
		defer wg.Done()
		for i := range c {

			select {
			case <-done:
				return
			case fannedIntStream <- i:
			}
		}
	}

	for _, c := range channels {
		wg.Add(1)
		// wait for each transfer to finish
		go transfer(c)
	}

	go func() {
		wg.Wait()
		close(fannedIntStream)
	}()

	return fannedIntStream

}

func generateInfStreamOfData() {

	start := time.Now()

	done := make(chan int)
	defer close(done)

	randomNumb := func() int {
		return rand.Intn(50000000)
	}

	randIntStream := repeatFunc(done, randomNumb)
	//primeStream := primeFinder(done, randIntStream)

	// 30.179117096s
	// It took 10 seconds only before it took 30

	// fanOut
	CPUCount := runtime.NumCPU()
	primeFinderChannels := make([]<-chan int, CPUCount)
	for i := 0; i < CPUCount; i++ {
		// if 6 CPUs then it will run 6 concurrent instances of primes
		primeFinderChannels[i] = primeFinder(done, randIntStream)
	}

	// fan in
	fannedInStream := fanIn(done, primeFinderChannels...)

	for rando := range take(done, fannedInStream, 10) {
		fmt.Println(rando)
	}

	fmt.Println(time.Since(start))
}

// FanOut-FanIn pattern

func dataInconsistency() {
	amount := 999
	go deductAmount(&amount, 1)
	go deductAmount(&amount, 1)
	go deductAmount(&amount, 1)

	/*
		We can solve the above problem with locks. Locks can have huge bottleneck and can slow down our goroutines


	*/

	// https://www.youtube.com/watch?v=Bk1c30avsuU

	start := time.Now()
	var wg sync.WaitGroup
	input := []int{1, 2, 3, 4, 5} // go routine for each value
	//result := []int{}             // shared result
	result := make([]int, len(input))

	for i, data := range input {
		wg.Add(1)
		// we are now confining to the result[i]
		go processData(&wg, &result[i], data)
	}

	wg.Wait() // need to wait for all the go to completes
	fmt.Println(time.Since(start))
	// comment out the lock and then uncomment.
	fmt.Println(result) // when you run it with forever you will see different data on each run.
	// You can solve this using mutex, but it'll bottleneck the code.
	// After the lock it's getting us the correct output, but each run has different order due to parallel go routines.
	// But this is not performant.
	// Without the lock it takes 2 seconds
	// It took 10 seconds with lock.

	// Confinement optimizes the program.
	// we want to confine the lock to the specific part of the go routine. Narrow it down as much as you can.
	// result can we concurrently write to each index of the result. For that we need to define the length of the array.
	// With the confinement it's ordered now.
}

var lock sync.Mutex

func process(data int) int {
	time.Sleep(time.Second * 2)
	return data * 2
}

func processData(wg *sync.WaitGroup, resultDest *int, data int) {
	//lock.Lock()
	defer wg.Done() // means the go routine is done when the func completes

	//processedData := data * 2

	// When you look into this code the critical section is when appending not processedData.
	processedData := process(data)
	// The below needs to lock not the process data.
	// Remember when you try to lock make sure your lock is very fine grain
	// put the lock here not on the start.
	//*result = append(*result, processedData) // multiple go routine is trying to add to the same data.

	*resultDest = processedData
	//lock.Unlock()
}

func deductAmount(amount *int, deductAmount int) {
	*amount -= deductAmount
}

// --------------------https://www.youtube.com/watch?v=bnbEULxcX3o--------------------------- //

// OrDoneChannel

var wg = sync.WaitGroup{}

func orDoneChannelFn() {

	// Hacky way of allowing all the types
	done := make(chan interface{}, 10)

	defer close(done)

	cows := make(chan interface{}, 100)
	pigs := make(chan interface{}, 100)

	go func() {
		for {
			select {
			case <-done:
				return
			case cows <- "moo":
			}
		}
	}()

	go func() {
		for {
			select {
			case <-done:
				return
			case pigs <- "oink":
			}
		}
	}()

	wg.Add(2)
	go consumerCows(done, cows)
	go consumerPigs(done, pigs)

	wg.Wait()
}

func consumerPigs(done <-chan interface{}, pigs <-chan interface{}) {

	defer wg.Done()

	for {
		select {
		case <-done:
			return
		case pig, ok := <-pigs:
			if !ok {
				println("channel closed")
			}
			println("Consume pigs..", pig)
		}
	}
}

func consumerCows(done <-chan interface{}, cows <-chan interface{}) {

	defer wg.Done()

	//for {
	//	select {
	//	case <-done:
	//		return
	//	case cow, ok := <-cows:
	//		if !ok {
	//			println("channel closed")
	//		}
	//		println("Consume cows..", cow)
	//	}
	//}

	for cow := range orDone(done, cows) {
		fmt.Println(cow)
	}
}

func orDone(done, generalChannel <-chan interface{}) <-chan interface{} {

	relayStream := make(chan interface{})

	go func() {
		defer close(relayStream)
		for {
			select {
			case <-done:
				return
			case v, ok := <-generalChannel:
				if !ok {
					println("Channel is closed")
					return
				}
				select {
				case relayStream <- v:
				case <-done:
					return
				}
			}
		}
	}()

	return relayStream
}

// Channel directions

func someFunc(num string) {
	fmt.Println(num)
}

func push(name string, ch chan string) {
	msg := "Hey, " + name
	ch <- msg
}

func starter(wg *sync.WaitGroup) {
	fmt.Println("This is the starter")
	defer wg.Done()
}

func starterWithChannel(ch chan string) {
	fmt.Println("This is the starter")
	ch <- "Hello, "
}
func follower(starter string) {
	fmt.Println(starter, "From the starter function, right now in follower")
}

func follow() {
	fmt.Println("This is the follower")
}
