package snapshot

import (
	"os"
	"time"

	"github.com/cosmos/iavl/v2"
	"github.com/cosmos/iavl/v2/metrics"
	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var log = zlog.Output(zerolog.ConsoleWriter{
	Out:        os.Stderr,
	TimeFormat: time.Stamp,
})

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
			log.Info().Msgf("found db paths: %v", paths)

			var (
				pool   = iavl.NewNodePool()
				done   = make(chan struct{})
				errors = make(chan error)
				cnt    = 0
			)
			for _, path := range paths {
				cnt++
				m := &metrics.TreeMetrics{}
				sqlOpts := iavl.SqliteDbOptions{Path: path, Metrics: m}
				sql, err := iavl.NewSqliteDb(pool, sqlOpts)
				if err != nil {
					return err
				}
				tree := iavl.NewTree(sql, pool, iavl.TreeOptions{Metrics: m})
				if err := tree.LoadVersion(version); err != nil {
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
