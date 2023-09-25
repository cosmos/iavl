package gen

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/cosmos/iavl/v2"
	"github.com/cosmos/iavl/v2/leveldb"
	"github.com/cosmos/iavl/v2/testutil"
	"github.com/dustin/go-humanize"
	"github.com/kocubinski/costor-api/compact"
	"github.com/kocubinski/costor-api/core"
	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var log = zlog.Output(zerolog.ConsoleWriter{
	Out:        os.Stderr,
	TimeFormat: time.Stamp,
})

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "gen",
		Short: "generate changesets",
	}

	cmd.AddCommand(emitCommand(), treeCommand())

	return cmd
}

func getChangesetIterator(typ string) (bench.ChangesetIterator, error) {
	switch typ {
	case "osmo-like":
		return testutil.OsmoLike().Iterator, nil
	case "height-zero":
		return testutil.NewTreeBuildOptions().Iterator, nil
	default:
		return nil, fmt.Errorf("unknown generator type %s", typ)
	}
}

func emitCommand() *cobra.Command {
	var (
		typ   string
		out   string
		start int
		limit int
	)
	cmd := &cobra.Command{
		Use:   "emit",
		Short: "emit generated changesets to disk",
		RunE: func(cmd *cobra.Command, args []string) error {
			itr, err := getChangesetIterator(typ)
			if err != nil {
				return err
			}
			ctx := core.Context{Context: cmd.Context()}

			stream := compact.StreamingContext{
				In:          make(chan compact.Sequenced),
				Context:     ctx,
				OutDir:      out,
				MaxFileSize: 100 * 1024 * 1024,
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				stats, err := stream.Compact()
				if err != nil {
					log.Fatal().Err(err).Msg("failed to compact")
				}
				log.Info().Msgf(stats.Report())
				wg.Done()
			}()

			var cnt int64
			for ; itr.Valid(); err = itr.Next() {
				if err != nil {
					return err
				}
				if limit > 0 && itr.Version() > int64(limit) {
					break
				}
				nodes := itr.Nodes()
				for ; nodes.Valid(); err = nodes.Next() {
					cnt++

					if itr.Version() < int64(start) {
						if cnt%5_000_000 == 0 {
							log.Info().Msgf("fast forward version=%d nodes=%s", itr.Version(), humanize.Comma(cnt))
						}
						continue
					}

					if cnt%500_000 == 0 {
						log.Info().Msgf("version=%d nodes=%s", itr.Version(), humanize.Comma(cnt))
					}

					select {
					case <-cmd.Context().Done():
						close(stream.In)
						wg.Wait()
						return nil
					default:
					}

					if err != nil {
						return err
					}
					stream.In <- nodes.GetNode()
				}
			}
			close(stream.In)
			wg.Wait()

			return nil
		},
	}

	cmd.Flags().StringVar(&typ, "type", "", "the type of changeset to generate")
	if err := cmd.MarkFlagRequired("type"); err != nil {
		panic(err)
	}
	cmd.Flags().StringVar(&out, "out", "", "the directory to write changesets to")
	if err := cmd.MarkFlagRequired("out"); err != nil {
		panic(err)
	}
	cmd.Flags().IntVar(&limit, "limit", -1, "the version (inclusive) to halt generation at. -1 means no limit")
	cmd.Flags().IntVar(&start, "start", 1, "the version (inclusive) to start generation at")

	return cmd
}

func treeCommand() *cobra.Command {
	var (
		dbPath  string
		genType string
		limit   int64
		useKv   bool
	)
	cmd := &cobra.Command{
		Use:   "tree",
		Short: "build and save a Tree to disk, taking generated changesets as input",
		RunE: func(cmd *cobra.Command, args []string) error {
			pool := iavl.NewNodePool()
			newSqlDb := true
			if useKv {
				newSqlDb = false
			}

			sql, err := iavl.NewSqliteDb(pool, dbPath, newSqlDb)
			if err != nil {
				return err
			}
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
			levelDb, err := leveldb.New("iavl-leveldb", dbPath)
			if err != nil {
				return err
			}

			if useKv {
				tree.SetKV(iavl.NewKvDB(levelDb, pool))
			}

			itr, err := getChangesetIterator(genType)
			if err != nil {
				return err
			}

			var i int64
			var lastHash []byte
			var lastVersion int64
			start := time.Now()
			for ; itr.Valid(); err = itr.Next() {
				if err != nil {
					return err
				}
				if limit > -1 && itr.Version() > limit {
					break
				}

				changeset := itr.Nodes()
				for ; changeset.Valid(); err = changeset.Next() {
					if err != nil {
						return err
					}
					node := changeset.GetNode()
					key := node.Key

					if node.Delete {
						_, _, err = tree.Remove(key)
						if err != nil {
							return err
						}
					} else {
						_, err = tree.Set(key, node.Value)
						if err != nil {
							return err
						}
					}

					i++
					if i%100_000 == 0 {
						log.Info().Msgf("leaves=%s dur=%s rate=%s version=%d",
							humanize.Comma(i),
							time.Since(start),
							humanize.Comma(int64(100_000/time.Since(start).Seconds())),
							itr.Version(),
						)
						start = time.Now()
					}
				}

				if useKv {
					lastHash, lastVersion, err = tree.SaveVersionKV()
				} else {
					lastHash, lastVersion, err = tree.SaveVersion()
				}
				if err != nil {
					return err
				}
			}

			log.Info().Msgf("last version=%d hash=%x", lastVersion, lastHash)

			return nil
		},
	}
	cmd.Flags().StringVar(&genType, "type", "", "the type of changeset to generate")
	if err := cmd.MarkFlagRequired("type"); err != nil {
		panic(err)
	}
	cmd.Flags().StringVar(&dbPath, "db", "/tmp", "the path to the database")
	cmd.Flags().Int64Var(&limit, "limit", -1, "the version (inclusive) to halt generation at. -1 means no limit")
	cmd.Flags().BoolVar(&useKv, "kv", false, "use leveldb as a kv database")
	return cmd
}
