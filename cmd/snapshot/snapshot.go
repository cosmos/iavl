package snapshot

import (
	"os"
	"time"

	"github.com/cosmos/iavl/v2"
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
			pool := iavl.NewNodePool()
			sqlOpts := iavl.SqliteDbOptions{Path: dbPath}
			sql, err := iavl.NewSqliteDb(pool, sqlOpts)
			defer func(sql *iavl.SqliteDb) {
				err = sql.Close()
				if err != nil {
					log.Error().Err(err).Msg("failed to close db")
				}
			}(sql)

			if err != nil {
				return err
			}
			tree := iavl.NewTree(sql, pool)
			if err = tree.LoadVersion(version); err != nil {
				return err
			}
			return sql.Snapshot(cmd.Context(), tree, version)
		},
	}
	cmd.Flags().Int64Var(&version, "version", 0, "version to snapshot")
	if err := cmd.MarkFlagRequired("version"); err != nil {
		panic(err)
	}
	cmd.Flags().StringVar(&dbPath, "db", "/tmp", "path to the sqlite database")
	return cmd
}
