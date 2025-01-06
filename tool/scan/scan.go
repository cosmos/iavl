package scan

import (
	"fmt"
	"os"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/cosmos/iavl/v2"
	"github.com/cosmos/iavl/v2/metrics"
	"github.com/spf13/cobra"
)

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use: "scan",
	}
	cmd.AddCommand(probeCommand(), rootsCommand())
	return cmd
}

func probeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "probe",
		Short: "prob sqlite cgo configuration",
		RunE: func(_ *cobra.Command, _ []string) error {
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

func rootsCommand() *cobra.Command {
	var (
		dbPath  string
		version int64
	)
	cmd := &cobra.Command{
		Use:   "roots",
		Short: "list roots",
		RunE: func(_ *cobra.Command, _ []string) error {
			sql, err := iavl.NewSqliteDb(iavl.NewNopNodePool(metrics.NilMetrics{}), iavl.SqliteDbOptions{Path: dbPath})
			if err != nil {
				return err
			}
			node, err := sql.LoadRoot(version)
			if err != nil {
				return err
			}
			fmt.Printf("root: %+v\n", node)
			return sql.Close()
		},
	}
	cmd.Flags().StringVar(&dbPath, "db", "", "path to sqlite db")
	cmd.Flags().Int64Var(&version, "version", 0, "version to query")
	if err := cmd.MarkFlagRequired("db"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("version"); err != nil {
		panic(err)
	}
	return cmd
}
