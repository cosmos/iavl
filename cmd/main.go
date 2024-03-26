package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
)

func main() {
	root := RootCommand()
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	defer func() {
		signal.Stop(signalChan)
		cancel()
	}()

	go func() {
		select {
		case <-signalChan:
			cancel()
		case <-ctx.Done():
		}
		<-signalChan
		os.Exit(2)
	}()

	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	}
}
