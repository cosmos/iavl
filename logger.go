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
