package v0

import (
	"bytes"
	"fmt"
	"sync"

	iavlv2 "github.com/cosmos/iavl/v2"
	"github.com/cosmos/iavl/v2/migrate/core"
	"github.com/kocubinski/costor-api/logz"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "v0",
		Short: "migrate latest iavl v0 application.db state to iavl v2 in sqlite",
	}
	cmd.AddCommand(allCommand(), snapshotCommand())
	return cmd
}

func snapshotCommand() *cobra.Command {
	var (
		dbv0         string
		snapshotPath string
		storekey     string
		concurrency  int
	)
	cmd := &cobra.Command{
		Use:   "snapshot",
		Short: "ingest latest iavl v0 application.db to a pre-order snapshot",
		RunE: func(cmd *cobra.Command, args []string) error {
			rs, err := core.NewReadonlyStore(dbv0)
			if err != nil {
				return err
			}

			var wg sync.WaitGroup

			var storeKeys []string
			if storekey != "" {
				storeKeys = []string{storekey}
			} else {
				for k, ci := range rs.CommitInfoByName() {
					storeKeys = append(storeKeys, k)
					log.Info().Msgf("store %s has commit info %v", k, ci)
				}
			}

			lock := make(chan struct{}, concurrency)
			for i := 0; i < concurrency; i++ {
				lock <- struct{}{}
			}

			for _, storeKey := range storeKeys {
				wg.Add(1)
				go func(sk string) {
					var count int64

					<-lock

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

					exporter, err := tree.ExportPreOrder()
					if err != nil {
						panic(err)
					}

					nextNodeFn := func() (*iavlv2.SnapshotNode, error) {
						count++
						exportNode, err := exporter.Next()
						if err != nil {
							log.Warn().Err(err).Msgf("export err after %d", count)
							return nil, err
						}
						return &iavlv2.SnapshotNode{
							Key:     exportNode.Key,
							Value:   exportNode.Value,
							Height:  exportNode.Height,
							Version: exportNode.Version,
						}, nil
					}

					root, err := iavlv2.IngestSnapshot(snapshotPath, sk, tree.Version(), nextNodeFn)
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

					lock <- struct{}{}
					wg.Done()
				}(storeKey)
			}

			wg.Wait()
			return nil
		},
	}

	cmd.Flags().StringVar(&dbv0, "db-v0", "", "Path to the v0 application.db")
	if err := cmd.MarkFlagRequired("db-v0"); err != nil {
		panic(err)
	}
	cmd.Flags().StringVar(&snapshotPath, "snapshot-path", "", "Path to the snapshot")
	if err := cmd.MarkFlagRequired("snapshot-path"); err != nil {
		panic(err)
	}
	cmd.Flags().IntVar(&concurrency, "concurrency", 6, "Number of concurrent migrations")
	cmd.Flags().StringVar(&storekey, "store-key", "", "Store key to migrate")

	return cmd
}

func allCommand() *cobra.Command {
	var (
		dbv0        string
		dbv2        string
		storekey    string
		concurrency int
	)
	cmd := &cobra.Command{
		Use:   "all",
		Short: "migrate latest iavl v0 application.db state to iavl v2 in sqlite",
		RunE: func(cmd *cobra.Command, args []string) error {
			rs, err := core.NewReadonlyStore(dbv0)
			if err != nil {
				return err
			}

			var wg sync.WaitGroup

			var storeKeys []string
			if storekey != "" {
				storeKeys = []string{storekey}
			} else {
				for k, ci := range rs.CommitInfoByName() {
					storeKeys = append(storeKeys, k)
					log.Info().Msgf("store %s has commit info %v", k, ci)
				}
			}

			lock := make(chan struct{}, concurrency)
			for i := 0; i < concurrency; i++ {
				lock <- struct{}{}
			}

			for _, storeKey := range storeKeys {
				wg.Add(1)
				go func(sk string) {
					var (
						count int64
						//since = time.Now()
					)

					<-lock

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
					sql, err := iavlv2.NewSqliteDb(iavlv2.NewNodePool(),
						iavlv2.SqliteDbOptions{
							Path:    fmt.Sprintf("%s/%s", dbv2, sk),
							WalSize: 1024 * 1024 * 1024,
						})
					if err != nil {
						panic(err)
					}
					exporter, err := tree.ExportPreOrder()
					if err != nil {
						panic(err)
					}

					nextNodeFn := func() (*iavlv2.SnapshotNode, error) {
						count++
						exportNode, err := exporter.Next()
						if err != nil {
							log.Warn().Err(err).Msgf("export err after %d", count)
							return nil, err
						}
						return &iavlv2.SnapshotNode{
							Key:     exportNode.Key,
							Value:   exportNode.Value,
							Height:  exportNode.Height,
							Version: exportNode.Version,
						}, nil
					}

					root, err := sql.WriteSnapshot(cmd.Context(), tree.Version(), nextNodeFn,
						iavlv2.SnapshotOptions{StoreLeafValues: true, SaveTree: true})
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
					if err := sql.Close(); err != nil {
						panic(err)
					}

					lock <- struct{}{}
					wg.Done()
				}(storeKey)
			}

			wg.Wait()
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
	cmd.Flags().IntVar(&concurrency, "concurrency", 6, "Number of concurrent migrations")

	return cmd
}
