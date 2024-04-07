package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func main() {

	/* https://www.youtube.com/watch?v=8omcakb31xQ */

	/*
		Contexts are immutable
		Async call graph.
		Helps to prevent leaks in goroutines and gracefully cancel the goroutines.

		1) It provides API to cancel the branches in call graph.
		2) Pass data in the call graph.

		done is builtin in context.
		context.Background --> empty context

	*/

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	// generators

	generator := func(dataItem string, stream chan interface{}) {
		for {
			select {
			case <-ctx.Done():
				return
			case stream <- dataItem:
			}
		}
	}

	infiniteApples := make(chan interface{})

	go generator("apple", infiniteApples)

	infiniteMangoes := make(chan interface{})

	go generator("mango", infiniteMangoes)

	infiniteKiwi := make(chan interface{})

	go generator("kiwi", infiniteKiwi)

	go func1(ctx, &wg, infiniteApples)

	func2 := genericFunc
	func3 := genericFunc

	wg.Add(1)

	go func2(ctx, &wg, infiniteMangoes)
	wg.Add(1)
	go func3(ctx, &wg, infiniteKiwi)

	wg.Wait()

}

func func1(ctx context.Context, parentWg *sync.WaitGroup, stream <-chan interface{}) {

	// func1 cancelled its children and main is still running.
	defer wg.Done()

	var wg sync.WaitGroup
	doWork := func(ctx context.Context) {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-stream:
				if !ok {
					fmt.Println("channel closed")
					return
				}
				println(d)
			}
		}
	}

	newCtx, cancel := context.WithTimeout(ctx, time.Second*20)
	defer cancel()

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go doWork(newCtx)
	}

	wg.Wait()

}

func genericFunc(ctx context.Context, wg *sync.WaitGroup, stream <-chan interface{}) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-stream:
			{
				if !ok {
					println("channel closed")
				}
				println(d)
			}
		}
	}
}
