package iavl

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"

	"github.com/cosmos/iavl/v2/metrics"
	"golang.org/x/exp/slices"
)

// MultiTree encapsulates multiple IAVL trees, each with its own "store key" in the context of the Cosmos SDK.
// Within IAVL v2 is only used to test the IAVL v2 implementation, and for import/export of IAVL v2 state.
type MultiTree struct {
	Trees map[string]*Tree

	pool             *NodePool
	rootPath         string
	treeOpts         TreeOptions
	shouldCheckpoint bool

	doneCh  chan saveVersionResult
	errorCh chan error
}

func NewMultiTree(rootPath string, opts TreeOptions) *MultiTree {
	return &MultiTree{
		Trees:    make(map[string]*Tree),
		doneCh:   make(chan saveVersionResult, 1000),
		errorCh:  make(chan error, 1000),
		treeOpts: opts,
		pool:     NewNodePool(),
		rootPath: rootPath,
	}
}

func ImportMultiTree(pool *NodePool, version int64, path string, treeOpts TreeOptions) (*MultiTree, error) {
	mt := NewMultiTree(path, treeOpts)
	paths, err := FindDbsInPath(path)
	if err != nil {
		return nil, err
	}
	var (
		cnt  = 0
		done = make(chan struct {
			path string
			tree *Tree
		})
		errs = make(chan error)
	)
	for _, dbPath := range paths {
		cnt++
		sql, err := NewSqliteDb(pool, defaultSqliteDbOptions(SqliteDbOptions{Path: dbPath}))
		if err != nil {
			return nil, err
		}
		go func(p string) {
			tree := NewTree(sql, pool, mt.treeOpts)
			importErr := tree.LoadSnapshot(version, PreOrder)

			if importErr != nil {
				errs <- fmt.Errorf("err while importing %s; %w", p, importErr)
				return
			}
			done <- struct {
				path string
				tree *Tree
			}{p, tree}
		}(dbPath)
	}

	for i := 0; i < cnt; i++ {
		select {
		case err = <-errs:
			return nil, err
		case res := <-done:
			prefix := filepath.Base(res.path)
			log.Info().Msgf("imported %s", prefix)
			mt.Trees[prefix] = res.tree
		}
	}

	return mt, nil
}

func (mt *MultiTree) MountTree(storeKey string) error {
	opts := defaultSqliteDbOptions(SqliteDbOptions{
		Path: mt.rootPath + "/" + storeKey,
	})
	sql, err := NewSqliteDb(mt.pool, opts)
	if err != nil {
		return err
	}
	tree := NewTree(sql, mt.pool, mt.treeOpts)
	mt.Trees[storeKey] = tree
	return nil
}

func (mt *MultiTree) MountTrees() error {
	paths, err := FindDbsInPath(mt.rootPath)
	if err != nil {
		return err
	}
	for _, dbPath := range paths {
		prefix := filepath.Base(dbPath)
		sqlOpts := defaultSqliteDbOptions(SqliteDbOptions{})
		sqlOpts.Path = dbPath
		log.Info().Msgf("mounting %s; opts %v", prefix, sqlOpts)
		sql, err := NewSqliteDb(mt.pool, sqlOpts)
		if err != nil {
			return err
		}
		tree := NewTree(sql, mt.pool, mt.treeOpts)
		mt.Trees[prefix] = tree
	}
	return nil
}

func (mt *MultiTree) LoadVersion(version int64) error {
	for k, tree := range mt.Trees {
		if err := tree.LoadVersion(version); err != nil {
			return fmt.Errorf("failed to load %s version %d; %w", k, version, err)
		}
	}
	return nil
}

func (mt *MultiTree) SaveVersion() ([]byte, int64, error) {
	version := int64(-1)
	for _, tree := range mt.Trees {
		_, v, err := tree.SaveVersion()
		if err != nil {
			return nil, 0, err
		}
		if version != -1 && version != v {
			return nil, 0, fmt.Errorf("unexpected; trees are at different versions: %d != %d", version, v)
		}
		version = v
	}
	return mt.Hash(), version, nil
}

type saveVersionResult struct {
	version int64
	hash    []byte
}

func (mt *MultiTree) SaveVersionConcurrently() ([]byte, int64, error) {
	treeCount := 0
	var workingSize atomic.Int64
	var workingBytes atomic.Uint64
	for _, tree := range mt.Trees {
		treeCount++
		go func(t *Tree) {
			t.shouldCheckpoint = mt.shouldCheckpoint
			h, v, err := t.SaveVersion()
			workingSize.Add(t.workingSize)
			workingBytes.Add(t.workingBytes)
			if err != nil {
				mt.errorCh <- err
			}
			mt.doneCh <- saveVersionResult{version: v, hash: h}
		}(tree)
	}

	var (
		errs    []error
		version = int64(-1)
	)
	for i := 0; i < treeCount; i++ {
		select {
		case err := <-mt.errorCh:
			log.Error().Err(err).Msg("failed to save version")
			errs = append(errs, err)
		case result := <-mt.doneCh:
			if version != -1 && version != result.version {
				errs = append(errs, fmt.Errorf("unexpected; trees are at different versions: %d != %d",
					version, result.version))
			}
			version = result.version
		}
	}
	mt.shouldCheckpoint = false

	if mt.treeOpts.MetricsProxy != nil {
		//bz := workingBytes.Load()
		//sz := workingSize.Load()
		//fmt.Printf("version=%d work-bytes=%s work-size=%s mem-ceiling=%s\n",
		//	version, humanize.IBytes(bz), humanize.Comma(sz), humanize.IBytes(mt.treeOpts.CheckpointMemory))
		mt.treeOpts.MetricsProxy.SetGauge(float32(workingBytes.Load()), "iavl_v2", "working_bytes")
		mt.treeOpts.MetricsProxy.SetGauge(float32(workingSize.Load()), "iavl_v2", "working_size")
	}

	if mt.treeOpts.CheckpointMemory > 0 && workingBytes.Load() >= mt.treeOpts.CheckpointMemory {
		mt.shouldCheckpoint = true
	}

	return mt.Hash(), version, errors.Join(errs...)
}

func (mt *MultiTree) SnapshotConcurrently() error {
	treeCount := 0
	for _, tree := range mt.Trees {
		treeCount++
		go func(t *Tree) {
			if err := t.SaveSnapshot(); err != nil {
				mt.errorCh <- err
			} else {
				mt.doneCh <- saveVersionResult{}
			}
		}(tree)
	}

	var errs []error
	for i := 0; i < treeCount; i++ {
		select {
		case err := <-mt.errorCh:
			log.Error().Err(err).Msg("failed to snapshot")
			errs = append(errs, err)
		case <-mt.doneCh:
		}
	}
	return errors.Join(errs...)
}

// Hash is a stand in for code at
// https://github.com/cosmos/cosmos-sdk/blob/80dd55f79bba8ab675610019a5764470a3e2fef9/store/types/commit_info.go#L30
// it used in testing. App chains should use the store hashing code referenced above instead.
func (mt *MultiTree) Hash() []byte {
	var (
		storeKeys []string
		hashes    []byte
	)
	for k := range mt.Trees {
		storeKeys = append(storeKeys, k)
	}

	slices.Sort(storeKeys)
	for _, k := range storeKeys {
		tree := mt.Trees[k]
		hashes = append(hashes, tree.root.hash...)
	}
	hash := sha256.Sum256(hashes)
	return hash[:]
}

func (mt *MultiTree) Close() error {
	for _, tree := range mt.Trees {
		if err := tree.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (mt *MultiTree) WarmLeaves() error {
	var cnt int
	for _, tree := range mt.Trees {
		cnt++
		go func(t *Tree) {
			if err := t.sql.WarmLeaves(); err != nil {
				mt.errorCh <- err
			} else {
				mt.doneCh <- saveVersionResult{}
			}
		}(tree)
	}
	for i := 0; i < cnt; i++ {
		select {
		case err := <-mt.errorCh:
			log.Error().Err(err).Msg("failed to warm leaves")
			return err
		case <-mt.doneCh:
		}
	}
	return nil
}

func (mt *MultiTree) QueryReport(bins int) error {
	m := &metrics.DbMetrics{}
	for _, tree := range mt.Trees {
		m.Add(tree.sql.metrics)
		tree.sql.metrics.SetQueryZero()
	}
	return m.QueryReport(bins)
}
