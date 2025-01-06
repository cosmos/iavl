package rollback

import (
	"github.com/cosmos/iavl/v2/metrics"
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
		version int
		path    string
	)
	cmd := &cobra.Command{
		Use:   "rollback",
		Short: "Rollback IAVL to a previous version",
		RunE: func(_ *cobra.Command, _ []string) error {
			dbPaths, err := iavl.FindDbsInPath(path)
			if err != nil {
				return err
			}
			for _, dbPath := range dbPaths {
				log.Info().Msgf("revert db %s to version %d", dbPath, version)
				sql, err := iavl.NewSqliteDb(iavl.NewNopNodePool(metrics.NilMetrics{}), iavl.SqliteDbOptions{Path: dbPath})
				if err != nil {
					return err
				}
				if err = sql.Revert(version); err != nil {
					return err
				}
				if err = sql.Close(); err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&path, "path", "", "Path to the IAVL database")
	cmd.Flags().IntVar(&version, "version", -1, "Version to rollback to")
	if err := cmd.MarkFlagRequired("path"); err != nil {
		return nil
	}
	if err := cmd.MarkFlagRequired("version"); err != nil {
		return nil
	}

	return cmd
}
