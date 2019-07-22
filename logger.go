package iavl

import (
	"fmt"
)

var (
	debugging = false
)

func debug(format string, args ...interface{}) {
	if debugging {
		fmt.Printf(format, args...)
	}
}

func startDebugger() {
	debugging = true
}

func endDebugger() {
	debugging = false
}
