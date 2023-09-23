package gen

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cosmos/iavl-bench/bench"
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

	cmd.AddCommand(emitCommand())

	return cmd
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
			var itr bench.ChangesetIterator

			switch typ {
			case "osmo-like":
				itr = testutil.OsmoLike().Iterator
			default:
				return fmt.Errorf("unknown generator type %s", typ)
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

			var err error
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
