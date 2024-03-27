package main

import (
	"fmt"
	"sync"
)

type KeyVal struct {
	key int
	val int
}

func ContainsDuplicate(nums []int) bool {

	// Shared map which maintains key, val
	// divide the array into fix intervals
	// concurrent func which checks if the value is in the shared map, if yes break then return true, otherwise add to the map.
	// if the func returns true then stop all the cooroutines and return true in the main func.

	interval := 2
	sharedMap := sync.Map{}

	var duplicateFound bool = false
	var wg sync.WaitGroup

	for i := 0; i < len(nums); i += interval {
		wg.Add(1)

		go func(start int) {
			defer wg.Done()
			subArray := nums[start:min(start+interval, len(nums))]

			for _, val := range subArray {
				if checkAndAdd(val, &sharedMap) {
					duplicateFound = true
					fmt.Println("Duplicate found! ", val)
					return
				}
			}

		}(i)

	}

	wg.Wait()

	return duplicateFound
}

func checkAndAdd(val int, sharedMap *sync.Map) bool {
	_, loaded := sharedMap.LoadOrStore(val, KeyVal{key: val, val: 0})
	return loaded
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
