package scan

import "github.com/spf13/cobra"

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use: "scan",
	}
	return cmd
}
