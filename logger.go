package iavl

import (
	"fmt"
)

func debug(format string, args ...interface{}) {
	if true {
		fmt.Printf(format, args...)
	}
}
