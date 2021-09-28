package iavl

import (
	"fmt"
)

var (
	Debugging = false
)

const (
	FlagIavlDebug        = "iavl-debug"
)

func debug(format string, args ...interface{}) {
	if Debugging {
		fmt.Printf(format, args...)
	}
}
