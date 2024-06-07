package scan

import (
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/kocubinski/costor-api/compact"
	"math"
	"os"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/cosmos/iavl/v2"
	"github.com/spf13/cobra"
)

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use: "scan",
	}
	cmd.AddCommand(probeCommand(), rootsCommand(), changesetCommand())
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

func rootsCommand() *cobra.Command {
	var (
		dbPath  string
		version int64
	)
	cmd := &cobra.Command{
		Use:   "roots",
		Short: "list roots",
		RunE: func(cmd *cobra.Command, args []string) error {
			sql, err := iavl.NewSqliteDb(iavl.NewNodePool(), iavl.SqliteDbOptions{Path: dbPath})
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
	cmd.MarkFlagRequired("db")
	cmd.MarkFlagRequired("version")
	return cmd
}

func changesetCommand() *cobra.Command {
	var (
		path  string
		until int64
	)
	cmd := &cobra.Command{
		Use:   "changeset",
		Short: "scan changesets, collect and print some simple statistics",
		RunE: func(cmd *cobra.Command, args []string) error {
			itr, err := compact.NewChangesetIterator(path)
			if err != nil {
				return err
			}
			var (
				count int64
				size  int64
				set   int64
				del   int64
			)
			prn := func() {
				fmt.Printf("count: %s, size: %s, sets: %s, deletes: %s\n",
					humanize.Comma(count), humanize.Bytes(uint64(size)), humanize.Comma(set), humanize.Comma(del))
			}
			for ; itr.Valid(); err = itr.Next() {
				if err != nil {
					return err
				}
				if itr.Version() > until {
					break
				}
				cs := itr.Nodes()
				for ; cs.Valid(); err = cs.Next() {
					if err != nil {
						return err
					}
					node := cs.GetNode()
					count++
					size += int64(len(node.Key) + len(node.Value))
					if node.Delete {
						del++
					} else {
						set++
					}
					if count%250000 == 0 {
						prn()
					}
				}
			}
			prn()
			return nil
		},
	}
	cmd.Flags().StringVar(&path, "path", "", "path to changeset")
	cmd.Flags().Int64Var(&until, "until", math.MaxInt64, "scan until version")
	if err := cmd.MarkFlagRequired("path"); err != nil {
		panic(err)
	}
	return cmd
}
