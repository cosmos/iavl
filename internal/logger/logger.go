package logger

import (
	"fmt"
)

const debugging = false

func Debug(format string, args ...interface{}) {
	if debugging {
		fmt.Printf(format, args...)
	}
}
