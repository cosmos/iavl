package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println("welcome home")
	fmt.Println(os.Args[1:])
}
