package main

import (
	"github.com/cosmos/iavl/v2/tool/bench"
	"github.com/cosmos/iavl/v2/tool/gen"
	"github.com/cosmos/iavl/v2/tool/rollback"
	"github.com/cosmos/iavl/v2/tool/scan"
	"github.com/cosmos/iavl/v2/tool/snapshot"
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
		bench.Command(),
		latestCommand(),
	)
	return cmd, nil
}
