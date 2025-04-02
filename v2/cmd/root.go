package main

import (
	"github.com/cosmos/iavl/v2/cmd/bench"
	"github.com/cosmos/iavl/v2/cmd/gen"
	"github.com/cosmos/iavl/v2/cmd/rollback"
	"github.com/cosmos/iavl/v2/cmd/scan"
	"github.com/cosmos/iavl/v2/cmd/snapshot"
	"github.com/spf13/cobra"
)

func RootCommand() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:   "iavl",
		Short: "benchmark cosmos/iavl",
	}
	cmd.AddCommand(
		gen.Command(),
		snapshot.Command(),
		rollback.Command(),
		scan.Command(),
		latestCommand(),
		bench.Command(),
	)
	return cmd, nil
}
