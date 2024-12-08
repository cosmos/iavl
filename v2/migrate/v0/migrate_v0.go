package v0

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	iavlv2 "github.com/cosmos/iavl/v2"
	"github.com/cosmos/iavl/v2/migrate/core"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/kocubinski/costor-api/logz"
	"github.com/spf13/cobra"
)

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "v0",
		Short: "migrate latest iavl v0 application.db state to iavl v2 in sqlite",
	}
	cmd.AddCommand(allCommand(), snapshotCommand(), metadataCommand(), latestVersionCommand())
	return cmd
}

const (
	latestVersionKey = "s/latest"
	commitInfoKeyFmt = "s/%d" // s/<version>
	appVersionKey    = "s/appversion"
)

func metadataCommand() *cobra.Command {
	var (
		dbv0 string
		dbv2 string
	)
	cmd := &cobra.Command{
		Use:   "v45-metadata",
		Short: "migrate CosmosSDK v0.45 store metadata stored in application.db state to iavl v2 in sqlite",
		RunE: func(cmd *cobra.Command, args []string) error {
			log := logz.Logger.With().Str("op", "migrate").Logger()

			v0, err := core.NewReadonlyStore(dbv0)
			if err != nil {
				return err
			}
			v2, err := iavlv2.NewSqliteKVStore(iavlv2.SqliteDbOptions{Path: dbv2})
			if err != nil {
				return err
			}
			bz, err := v0.Get([]byte(latestVersionKey))
			if err != nil {
				return err
			}
			i64 := &types.Int64Value{}
			err = proto.Unmarshal(bz, i64)
			if err != nil {
				return err
			}
			log.Info().Msgf("latest version: %d\n", i64.Value)
			if err = v2.Set([]byte(latestVersionKey), bz); err != nil {
				return err
			}

			bz, err = v0.Get([]byte(fmt.Sprintf(commitInfoKeyFmt, i64.Value)))
			if err != nil {
				return err
			}
			commitInfo := &CommitInfo{}
			if err = proto.Unmarshal(bz, commitInfo); err != nil {
				return err
			}
			if err = v2.Set([]byte(fmt.Sprintf(commitInfoKeyFmt, i64.Value)), bz); err != nil {
				return err
			}

			bz, err = v0.Get([]byte(appVersionKey))
			if err != nil {
				return err
			}
			if err = v2.Set([]byte(appVersionKey), bz); err != nil {
				return err
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&dbv0, "db-v0", "", "Path to the v0 application.db")
	cmd.Flags().StringVar(&dbv2, "db-v2", "", "Path to the v2 root")
	if err := cmd.MarkFlagRequired("db-v0"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("db-v2"); err != nil {
		panic(err)
	}
	return cmd
}

func latestVersionCommand() *cobra.Command {
	var (
		db      string
		version int
		set     bool
	)
	cmd := &cobra.Command{
		Use:   "latest-version",
		Short: "get/set the latest version in the metadata.sqlite",
		RunE: func(cmd *cobra.Command, args []string) error {
			kv, err := iavlv2.NewSqliteKVStore(iavlv2.SqliteDbOptions{Path: db})
			if err != nil {
				return err
			}
			if set && version == -1 {
				return errors.New("version must be set")
			}
			if set {

			} else {
				bz, err := kv.Get([]byte(latestVersionKey))
				if err != nil {
					return err
				}
				i64 := &types.Int64Value{}
				err = proto.Unmarshal(bz, i64)
				if err != nil {
					return err
				}
				fmt.Printf("latest version: %d\n", i64.Value)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&db, "db", "", "Path to the metadata.sqlite")
	if err := cmd.MarkFlagRequired("db"); err != nil {
		panic(err)
	}
	cmd.Flags().IntVar(&version, "version", -1, "Version to set")
	cmd.Flags().BoolVar(&set, "set", false, "Set the latest version")
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
				for k := range rs.CommitInfoByName() {
					storeKeys = append(storeKeys, k)
				}
			}

			lock := make(chan struct{}, concurrency)
			for i := 0; i < concurrency; i++ {
				lock <- struct{}{}
			}

			// init db and close
			initConn, err := iavlv2.NewIngestSnapshotConnection(snapshotPath)
			if err != nil {
				return err
			}
			if err = initConn.Close(); err != nil {
				return err
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

					conn, err := iavlv2.NewIngestSnapshotConnection(snapshotPath)
					if err != nil {
						panic(err)
					}
					root, err := iavlv2.IngestSnapshot(conn, sk, tree.Version(), nextNodeFn)
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
				for k := range rs.CommitInfoByName() {
					storeKeys = append(storeKeys, k)
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
						iavlv2.SnapshotOptions{StoreLeafValues: true, WriteCheckpoint: true})
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
