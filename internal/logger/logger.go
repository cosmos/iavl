package logger

import (
	"fmt"
)

var debugging = true

func Debug(format string, args ...interface{}) {
	if debugging {
		fmt.Printf(format, args...)
	}
}
