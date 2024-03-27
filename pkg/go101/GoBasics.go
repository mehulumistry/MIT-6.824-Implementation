package main

import (
	"fmt"
	"math"
	"runtime"
)

// defer similar to finally in java

// Go doesn't have classes.

// Making a variable accessible to other packages

//This is called exporting. To define a variable in the package scope,
//you can only use a long declaration or a multiple declaration.
//After that, you need to capitalize its first letter

// * --> follow the address
// & --> Take the address

func main2() {

	x := "Alisha"
	y := x
	println(x, y)

	fmt.Println(pow(4, 5, 10))

	//panicCheck()

	//divideFnWOErr(4, 0)

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from println", r)
		}
	}()

	result, err := divideFn(4, 0)
	if err != nil {
		// .*(Type) is used in go for type assertions.
		// Refer TypeAssertion
		if divError, ok := err.(*DivisionByZeroError); ok {
			fmt.Println(divError.Error())
			fmt.Println("Ok: ", ok)
		} else {
			fmt.Println("Unexpected error: ", err)
		}
	} else {
		fmt.Println("Division result: ", result)
	}

	typeAssertionFn()

}

var ExportedVariable = "ExportedVariable"

type DivisionByZeroError struct {
	Msg string
}

func (e *DivisionByZeroError) Error() string {
	return e.Msg
}

func divideFnWOErr(a, b int) int {
	return a / b
}

func typeAssertionFn() {

	var i interface{} = "hello"

	// hello
	s := i.(string)
	fmt.Println("simple assert:", s)

	// hello, true
	s, ok := i.(string)
	fmt.Println(s, ok)

	// 0, false
	f, ok := i.(float64)
	fmt.Println(f, ok)

	// interface {} is string, not float64
	//f = i.(float64)
	//fmt.Println(f)

	v := Vertex{4, 5}
	p := &Vertex{4, 5}

	//fmt.Println(v.Abs())
	//Scale(&v, 10)
	//fmt.Println("Scale: ", &v)
	//fmt.Println(v.Abs())
	p.ScaleFn(3)
	v.ScaleFn(3)
	(*(&p)).ScaleFn(5)

	var hello = T{"Hello"}
	hello.M()
	fmt.Println(p, v, p, hello)

}

func divideFn(a, b int) (int, error) {

	if b == 0 {
		return 0, &DivisionByZeroError{Msg: "Division by zero is not allowed"}
	} else {
		return a / b, nil
	}

}

func panicCheck() {

	var run func() = nil
	defer run()

	fmt.Println("runs")

}

func loopOverHelper(x int) {
	sum := 0
	for i := 0; i < 10; i++ {
		sum += 1
	}
	fmt.Println(sum)
}

func pow(x, n, lim float64) float64 {

	if v := math.Pow(x, n); v < lim {
		return v
	} else {
		fmt.Printf("%g >= %g\n\n", v, lim)
	}

	fmt.Println("Go runs on....")

	switch os := runtime.GOOS; os {
	case "darwin":
		fmt.Println("darwin")
	case "linux":
		fmt.Println("darwin")
	default:
		fmt.Println("%s \n", os)
	}

	return lim

}

type I interface {
	M()
}

type T struct {
	S string
}

// M Initialize interface
func (t T) M() {
	fmt.Println("Hello")
}

// Methods are like special functions; very similar to class in Java.

type Vertex struct {
	X, Y float64
}

// Abs
//
// class Vertex {
//
//		float64 x, y;
//
//		Abs() {
//			return math.Sqrt(v.X*v.X + v.Y*v.Y)
//		}
//	}
//
// Methods are functions with a receiver argument.
// when receiver function then you can do receiver.Fn()
func (v Vertex) Abs() float64 {
	return math.Sqrt(v.X*v.X + v.Y*v.Y)
}

func Abs2(v Vertex) float64 {
	return math.Sqrt(v.X*v.X + v.Y*v.Y)
}

// Scale * is like this pass an instance.
func Scale(v *Vertex, f float64) {
	v.X = v.X * f
	v.Y = v.Y * f
}

func (v *Vertex) ScaleFn(f float64) {
	v.X = v.X * f
	v.Y = v.Y * f
}
