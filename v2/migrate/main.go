package main

import (
	"fmt"
	"os"

	migratev1 "github.com/cosmos/iavl/v2/migrate/v1"
	"github.com/spf13/cobra"
)

func main() {
	root := cobra.Command{
		Use:   "migrate",
		Short: "migrate application.db to IAVL v2",
	}
	root.AddCommand(migratev1.Command())

	if err := root.Execute(); err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		os.Exit(1)
	}
}
