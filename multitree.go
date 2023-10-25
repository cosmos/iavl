package iavl

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"slices"

	"github.com/cosmos/iavl/v2/metrics"
)

type MultiTree struct {
	Trees map[string]*Tree

	sqlOpts SqliteDbOptions
	pool    *NodePool
	metrics *metrics.TreeMetrics
	// TODO
	// remove, each tree should get its own cache.
	// pull cache metrics up
	cache *NodeCache

	doneCh  chan saveVersionResult
	errorCh chan error
}

func NewMultiTree() *MultiTree {
	return &MultiTree{
		Trees:   make(map[string]*Tree),
		doneCh:  make(chan saveVersionResult, 1000),
		errorCh: make(chan error, 1000),
		metrics: &metrics.TreeMetrics{},
		cache:   NewNodeCache(),
	}
}

func NewStdMultiTree(sqlOpts SqliteDbOptions, pool *NodePool) *MultiTree {
	tree := NewMultiTree()
	tree.sqlOpts = sqlOpts
	tree.pool = pool
	return tree
}

func ImportMultiTree(pool *NodePool, version int64, path string) (*MultiTree, error) {
	mt := NewMultiTree()
	nodeCache := NewNodeCache()
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
		sql, err := NewSqliteDb(pool, defaultSqliteDbOptions(SqliteDbOptions{Path: dbPath, Metrics: mt.metrics}))
		if err != nil {
			return nil, err
		}
		go func(p string) {
			root, importErr := sql.ImportSnapshot(version, false)
			tree := &Tree{
				root:           root,
				version:        version,
				lastCheckpoint: version,
				pool:           pool,
				cache:          nodeCache,
				sql:            sql,
				metrics:        mt.metrics,
			}
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

func (mt *MultiTree) AddTree(storeKey string) error {
	opts := *&mt.sqlOpts
	opts.Path = opts.Path + "/" + storeKey
	sql, err := NewSqliteDb(mt.pool, opts)
	if err != nil {
		return err
	}
	tree := NewTree(sql, mt.pool)
	mt.Trees[storeKey] = tree
	return nil
}

func (mt *MultiTree) MountTrees(path string) error {
	paths, err := FindDbsInPath(path)
	if err != nil {
		return err
	}
	sqlOpts := defaultSqliteDbOptions(SqliteDbOptions{Metrics: mt.metrics})
	for _, dbPath := range paths {
		prefix := filepath.Base(dbPath)
		opts := *&sqlOpts
		opts.Path = dbPath
		log.Info().Msgf("mounting %s; opts %v", prefix, opts)
		sql, err := NewSqliteDb(mt.pool, opts)
		if err != nil {
			return err
		}
		tree := &Tree{sql: sql, pool: mt.pool, metrics: mt.metrics, cache: mt.cache}
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
	for _, tree := range mt.Trees {
		treeCount++
		go func(t *Tree) {
			h, v, err := t.SaveVersion()
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
	return mt.Hash(), version, errors.Join(errs...)
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
		if err := tree.sql.Close(); err != nil {
			return err
		}
	}
	return nil
}
