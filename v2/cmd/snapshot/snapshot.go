package snapshot

import (
	"fmt"

	"github.com/cosmos/iavl/v2"
	"github.com/spf13/cobra"
)

var log = iavl.NewTestLogger()

func Command() *cobra.Command {
	var (
		version int64
		dbPath  string
	)
	cmd := &cobra.Command{
		Use:   "snapshot",
		Short: "take a snapshot of the tree at version n and write to SQLite",
		RunE: func(cmd *cobra.Command, args []string) error {
			paths, err := iavl.FindDbsInPath(dbPath)
			if err != nil {
				return err
			}
			log.Info(fmt.Sprintf("found db paths: %v", paths))

			var (
				pool   = iavl.NewNodePool()
				done   = make(chan struct{})
				errors = make(chan error)
				cnt    = 0
			)
			for _, path := range paths {
				cnt++
				sqlOpts := iavl.SqliteDbOptions{Path: path}
				sql, err := iavl.NewSqliteDb(pool, sqlOpts)
				if err != nil {
					return err
				}
				tree := iavl.NewTree(sql, pool, iavl.TreeOptions{})
				if err = tree.LoadVersion(version); err != nil {
					return err
				}
				go func() {
					snapshotErr := sql.Snapshot(cmd.Context(), tree)
					if snapshotErr != nil {
						errors <- snapshotErr
					}
					snapshotErr = sql.Close()
					if snapshotErr != nil {
						errors <- snapshotErr
					}
					done <- struct{}{}
				}()
			}
			for i := 0; i < cnt; i++ {
				select {
				case <-done:
					continue
				case err := <-errors:
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().Int64Var(&version, "version", 0, "version to snapshot")
	if err := cmd.MarkFlagRequired("version"); err != nil {
		panic(err)
	}
	cmd.Flags().StringVar(&dbPath, "db", "/tmp", "path to the sqlite database")
	return cmd
}
