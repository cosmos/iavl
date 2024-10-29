package main

import (
	"github.com/cosmos/iavl/v2"
	"github.com/spf13/cobra"
)

func latestCommand() *cobra.Command {
	var (
		dbPath  string
		version int64
	)
	cmd := &cobra.Command{
		Use:   "latest",
		Short: "fill the latest table with the latest version of leaf nodes in a tree",
		RunE: func(cmd *cobra.Command, args []string) error {
			paths, err := iavl.FindDbsInPath(dbPath)
			if err != nil {
				return err
			}
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
					fillErr := tree.WriteLatestLeaves()
					if fillErr != nil {
						errors <- fillErr
					}
					fillErr = tree.Close()
					if fillErr != nil {
						errors <- fillErr
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
	cmd.Flags().StringVar(&dbPath, "db", "", "the path to the db to fill the latest table for")
	if err := cmd.MarkFlagRequired("db"); err != nil {
		panic(err)
	}
	cmd.Flags().Int64Var(&version, "version", 0, "version to fill from")
	if err := cmd.MarkFlagRequired("version"); err != nil {
		panic(err)
	}
	return cmd
}
