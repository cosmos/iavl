package iavl

import (
	"fmt"
)

var (
	debugging = false
)

const (
	FlagIavlDebug        = "iavl-debug"
)

func debug(format string, args ...interface{}) {
	if debugging {
		fmt.Printf(format, args...)
	}
}
