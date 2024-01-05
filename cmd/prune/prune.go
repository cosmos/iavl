package prune

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
		path    string
	)
	cmd := &cobra.Command{
		Use:   "prune",
		Short: "Prune IAVL to a previous version",
		RunE: func(cmd *cobra.Command, args []string) error {
			dbPaths, err := iavl.FindDbsInPath(path)
			if err != nil {
				return err
			}
			for _, dbPath := range dbPaths {
				log.Info().Msgf("prune db %s to version %d", dbPath, version)
				pool := iavl.NewNodePool()
				sql, err := iavl.NewSqliteDb(pool, iavl.SqliteDbOptions{Path: dbPath, WalSize: 800 * 1024 * 1024})
				if err != nil {
					return err
				}
				if err := sql.ResetShardQueries(); err != nil {
					return err
				}
				if err := sql.DeleteVersionsTo_V3(version); err != nil {
					return err
				}
				if err := sql.Close(); err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().Int64Var(&version, "version", -1, "Version to rollback to")
	cmd.Flags().StringVar(&path, "path", "", "Path to the IAVL database")
	if err := cmd.MarkFlagRequired("version"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("path"); err != nil {
		panic(err)
	}
	return cmd
}
