package rollback

import (
	"fmt"

	"github.com/cosmos/iavl/v2"
	"github.com/spf13/cobra"
)

var log = iavl.NewTestLogger()

func Command() *cobra.Command {
	var (
		version int
		path    string
	)
	cmd := &cobra.Command{
		Use:   "rollback",
		Short: "Rollback IAVL to a previous version",
		RunE: func(cmd *cobra.Command, args []string) error {
			dbPaths, err := iavl.FindDbsInPath(path)
			if err != nil {
				return err
			}
			for _, dbPath := range dbPaths {
				log.Info(fmt.Sprintf("revert db %s to version %d", dbPath, version))
				sql, err := iavl.NewSqliteDb(iavl.NewNodePool(), iavl.SqliteDbOptions{Path: dbPath})
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
