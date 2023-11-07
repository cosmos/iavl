package v0

import (
	"bytes"
	"fmt"
	"sync"

	iavlv2 "github.com/cosmos/iavl/v2"
	"github.com/cosmos/iavl/v2/migrate/core"
	"github.com/kocubinski/costor-api/logz"
	"github.com/spf13/cobra"
)

func Command() *cobra.Command {
	var (
		dbv0     string
		dbv2     string
		storekey string
	)
	cmd := &cobra.Command{
		Use:   "v0",
		Short: "migrate latest iavl v0 application.db state to iavl v2 in sqlite",
		RunE: func(cmd *cobra.Command, args []string) error {
			multitree := iavlv2.NewMultiTree(dbv2, iavlv2.TreeOptions{})
			rs, err := core.NewReadonlyStore(dbv0)
			if err != nil {
				return err
			}

			//sampleRate := int64(10_000)
			var wg sync.WaitGroup

			var storeKeys []string
			if storekey != "" {
				storeKeys = []string{storekey}
			} else {
				for k := range rs.CommitInfoByName() {
					storeKeys = append(storeKeys, k)
				}
			}

			for _, storeKey := range storeKeys {
				wg.Add(1)
				go func(sk string) {
					var (
						count int64
						//since = time.Now()
					)

					log := logz.Logger.With().Str("store", sk).Logger()
					log.Info().Msgf("migrating %s", sk)

					s, err := core.NewReadonlyStore(dbv0)
					if err != nil {
						panic(err)
					}
					_, tree, err := s.LatestTree(sk)
					if err != nil {
						log.Warn().Err(err).Msgf("skipping %s", sk)
						wg.Done()
						return
					}
					if err = multitree.MountTree(sk); err != nil {
						panic(err)
					}

					v2Tree := multitree.Trees[sk]
					exporter, err := tree.ExportPreOrder()
					//exporter, err := tree.Export()
					if err != nil {
						panic(err)
					}

					nextNodeFn := func() *iavlv2.SnapshotNode {
						count++
						exportNode, err := exporter.Next()
						if err != nil {
							panic(err)
						}
						return &iavlv2.SnapshotNode{
							Key:     exportNode.Key,
							Value:   exportNode.Value,
							Height:  exportNode.Height,
							Version: exportNode.Version,
						}
					}

					sql := v2Tree.GetSql()
					root, err := sql.WriteSnapshot(cmd.Context(), tree.Version(), true, nextNodeFn)
					if err != nil {
						panic(err)
					}

					v0Hash, err := tree.WorkingHash()
					if err != nil {
						panic(err)
					}
					if !bytes.Equal(root.GetHash(), v0Hash) {
						panic(fmt.Sprintf("v2 hash=%x != v0 hash=%x", root.GetHash(), v0Hash))
					}

					if err = sql.SaveRoot(tree.Version(), root); err != nil {
						panic(err)
					}

					_, err = sql.ImportSnapshotFromTable(tree.Version(), true)
					if err != nil {
						panic(err)
					}

					wg.Done()
				}(storeKey)
			}

			wg.Wait()

			if err != nil {
				return err
			}
			//logz.Logger.Info().Msgf("saved v2 with hash %x", multitree.Hash())
			if err = multitree.Close(); err != nil {
				return err
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&dbv0, "db-v0", "", "Path to the v0 application.db")
	cmd.Flags().StringVar(&dbv2, "db-v2", "", "Path to the v2 root")
	cmd.Flags().StringVar(&storekey, "store-key", "", "Store key to migrate")
	if err := cmd.MarkFlagRequired("db-v0"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("db-v2"); err != nil {
		panic(err)
	}

	return cmd
}
