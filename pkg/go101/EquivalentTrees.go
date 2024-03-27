package main

/*

Equivalent of trees uses O(N) approach in most of the non-concurrent programs.

Because you need to traverse the whole tree and in other tree you can see if any mismatch.

But with concurrent programs if you start traversing both the trees in go routine then you can break early if any mismatch
No need to traverse whole tree.

*/

import (
	"fmt"
	"golang.org/x/tour/tree"
)

type Tree struct {
	Left  *Tree
	Value int
	Right *Tree
}

// Walk walks the tree t sending all values
// from the tree to the channel ch.
func Walk(t *tree.Tree, ch chan int) {
	if t == nil {
		return
	}
	Walk(t.Left, ch)
	fmt.Println("Value: ", t.Value)
	ch <- t.Value
	Walk(t.Right, ch)
}

func Walking(t *tree.Tree, ch chan int) {
	Walk(t, ch)
	defer close(ch)
}

// Same determines whether the trees
// t1 and t2 contain the same values.
func Same(t1, t2 *tree.Tree) bool {

	treeChannel1 := make(chan int)
	treeChannel2 := make(chan int)

	go Walking(t1, treeChannel1)
	go Walking(t2, treeChannel2)

	for {
		// To test whether the channel is close or not
		v1, ok1 := <-treeChannel1
		v2, ok2 := <-treeChannel2

		if ok1 != ok2 || v1 != v2 {
			return false
		}

		if !ok1 {
			break
		}

	}

	return true
}

func main() {
	//println(Same(tree.New(1), tree.New(2)))
	println(Same(tree.New(1), tree.New(2)))
}
