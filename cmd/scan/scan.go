package scan

import (
	"fmt"
	"os"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/spf13/cobra"
)

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use: "scan",
	}
	cmd.AddCommand(probeCommand())
	return cmd
}

func probeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "probe",
		Short: "prob sqlite cgo configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			f, err := os.CreateTemp("", "iavl-v2-probe.sqlite")
			if err != nil {
				return err
			}
			fn := f.Name()
			fmt.Println("fn:", fn)
			conn, err := sqlite3.Open(fn)
			if err != nil {
				return err
			}

			stmt, err := conn.Prepare("PRAGMA mmap_size=1000000000000")
			if err != nil {
				return err
			}
			_, err = stmt.Step()
			if err != nil {
				return err
			}
			if err = stmt.Close(); err != nil {
				return err
			}

			stmt, err = conn.Prepare("PRAGMA mmap_size")
			if err != nil {
				return err
			}
			_, err = stmt.Step()
			if err != nil {
				return err
			}
			res, _, err := stmt.ColumnRawString(0)
			if err != nil {
				return err
			}
			fmt.Println("mmap:", res)

			if err = stmt.Close(); err != nil {
				return err
			}
			if err = conn.Close(); err != nil {
				return err
			}
			if err = os.Remove(f.Name()); err != nil {
				return err
			}
			return nil
		},
	}
	return cmd
}
