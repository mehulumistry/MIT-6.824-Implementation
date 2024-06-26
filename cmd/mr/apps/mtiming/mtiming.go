package main

//
// a MapReduce pseudo-application to test that workers
// execute map tasks in parallel.
//
// go build -buildmode=plugin mtiming.go
//

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/mehulumistry/MIT-6.824-Implementation/pkg/mr"
)

func nparallel(phase string) int {
	// create a file so that other workers will see that
	// we're running at the same time as them.
	pid := os.Getpid()
	myfilename := fmt.Sprintf("mr-worker-%s-%d", phase, pid)
	err := os.WriteFile(myfilename, []byte("x"), 0666)

	//println("Created file name in map: ", myfilename)
	if err != nil {
		panic(err)
	}

	// are any other workers running?
	// find their PIDs by scanning directory for mr-worker-XXX files.
	dd, err := os.Open(".")
	if err != nil {
		panic(err)
	}
	names, err := dd.Readdirnames(1000000)
	if err != nil {
		panic(err)
	}
	ret := 0
	for _, name := range names {
		var xpid int
		pat := fmt.Sprintf("mr-worker-%s-%%d", phase)
		n, err := fmt.Sscanf(name, pat, &xpid)
		if n == 1 && err == nil {
			err := syscall.Kill(xpid, 0)
			if err == nil {
				// if err == nil, xpid is alive.
				ret += 1
			}
		}
	}
	errF := dd.Close()
	if errF != nil {
		return 0
	}

	time.Sleep(1 * time.Second)

	err = os.Remove(myfilename)
	if err != nil {
		panic(err)
	}

	return ret
}

func Map(filename string, contents string) []mr.KeyValue {
	t0 := time.Now()
	ts := float64(t0.Unix()) + (float64(t0.Nanosecond()) / 1000000000.0)
	pid := os.Getpid()

	n := nparallel("map")

	kva := []mr.KeyValue{}

	// for every file we are inserting two values
	// {times-pid, currentTime}
	// {parallel-pid, }
	kva = append(kva, mr.KeyValue{
		fmt.Sprintf("times-%v", pid),
		fmt.Sprintf("%.1f", ts)})
	kva = append(kva, mr.KeyValue{
		fmt.Sprintf("parallel-%v", pid),
		fmt.Sprintf("%d", n)})

	return kva
}

func Reduce(key string, values []string) string {
	//n := nparallel("reduce")

	// sort values to ensure deterministic output.
	vv := make([]string, len(values))
	copy(vv, values)
	sort.Strings(vv)

	val := strings.Join(vv, " ")
	return val
}
