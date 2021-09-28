package iavl

import (
	"fmt"
)

var (
	Debugging = 0
)

const (
	FlagIavlDebug        = "iavl-debug"
	LEVEL0 = 0
	LEVEL1 = 1
	LEVEL2 = 2
)

func debug(level int, format string, args ...interface{}) {
	if Debugging >= level {
		fmt.Printf(format, args...)
	}
}
