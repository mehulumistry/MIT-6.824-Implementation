package mr

import (
	"fmt"
	"os"
)

func HandleError(err error) {
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
}

var enableOutput bool

func EnableLogging(output bool) {
	// Logic to set enableOutput based on config file, environment variable, etc.
	enableOutput = output
}

func MRPrintln(format string, args ...interface{}) {
	if enableOutput {
		fmt.Printf(format, args...)
	}
}
