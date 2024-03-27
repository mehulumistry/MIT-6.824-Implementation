package main

import (
	"fmt"
	"golang.org/x/tour/pic"
	"golang.org/x/tour/reader"
	"golang.org/x/tour/wc"
	"math/rand"
	"strings"
	"sync"
)

func main() {

	// fmt.Println(ExportedVariable)
	// slicesExe()
	// wordCountExe()
	//fiboExe()
	//array := []int{1, 2, 3, 1}
	//duplicateFound := ContainsDuplicate(array)
	//fmt.Println(duplicateFound)

	//fmt.Println(IsAnagram("a", "ab"))
	parent()

}

func Pic(dx, dy int) [][]uint8 {

	// [[][]] 1D array slices of slice
	pictureData := make([][]uint8, dy)

	for indx, value := range pictureData {
		pictureData[indx] = make([]uint8, dx)
		fmt.Printf("Indx: %s Value: %s\n", indx, value)
	}

	for y := range pictureData {
		for x := range pictureData[y] {
			pictureData[y][x] = uint8(x + y/2)
		}
	}

	return pictureData
}

func slicesExe() {
	pic.Show(Pic)
}

func WordCount(s string) map[string]int {

	// slice by space of string
	words := strings.Fields(s)
	fmt.Println("Words: ", words)

	// Iterate over the slice and append to the map.

	wordCounts := make(map[string]int)
	for _, word := range words {
		wordCounts[word]++
	}

	return wordCounts
}

func wordCountExe() {
	wc.Test(WordCount)
}

func fibonacci() func() int {
	a, b := 0, 1

	return func() int {
		a, b = b, a+b
		return a
	}
}

func fiboExe() {
	f := fibonacci()
	for i := 0; i < 10; i++ {
		fmt.Println(f())
	}
}

type IPAddr [4]byte

// Similar to how you override in java. Now when you print IP it will print 1.1.1.1
func (ipAddr IPAddr) String() string {
	return fmt.Sprintf("%v.%v.%v.%v", ipAddr[0], ipAddr[1], ipAddr[2], ipAddr[3])
}

// similar to string we have Error()
//type error interface {
//	Error() string
//}

func stringerExercise() {
	hosts := map[string]IPAddr{
		"loopback":  {127, 0, 0, 1},
		"googleDNS": {8, 8, 8, 8},
	}
	for name, ip := range hosts {
		fmt.Printf("%v: %v\n", name, ip)
	}
}

// The io.Reader interface has a Read method:
//
// func (T) Read(b []byte) (n int, err error)

type MyReader struct{}

func (m MyReader) Read(p []byte) (n int, err error) {

	for i := range p {
		p[i] = 'A'
	}

	return len(p), nil
}

// TODO: Add a Read([]byte) (int, error) method to MyReader.

func readerValidateExe() {
	reader.Validate(MyReader{})
}

func task() {
	println("Task Done...")
}

func parent() {

	var wg sync.WaitGroup
	wg.Add(10)

	defer wg.Done()

	doTask := make(chan bool)

	defer close(doTask)

	go func() {
		v, ok := <-doTask
		task()
		if !ok {
			fmt.Println(ok)
		}
		fmt.Println(v)
		wg.Wait()
		close(doTask)
	}()

	for i := 0; i < 10; i++ {
		go lazyGophers(doTask, &wg)
	}

}

func lazyGophers(task chan bool, wg *sync.WaitGroup) {

	defer wg.Done()

	randInt := rand.Intn(5)
	println("Call us lazy gophers....", randInt)

	if randInt == 3 {
		task <- true
		return
	}

}

// Closures

func activateGiftCard() func(int) int {

	amount := 100

	debitFunc := func(debitAmount int) int {
		amount -= debitAmount
		return amount
	}

	return debitFunc
}
