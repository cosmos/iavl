package iavl

import (
	"crypto/sha256"
	"fmt"
	"slices"

	"github.com/cosmos/iavl/v2/metrics"
)

type MultiTree struct {
	Trees map[string]*Tree

	sqlOpts SqliteDbOptions
	pool    *NodePool
	metrics *metrics.TreeMetrics
}

func NewMultiTree() *MultiTree {
	return &MultiTree{
		Trees: make(map[string]*Tree),
	}
}

func NewStdMultiTree(sqlOpts SqliteDbOptions, pool *NodePool) *MultiTree {
	return &MultiTree{
		Trees:   make(map[string]*Tree),
		sqlOpts: sqlOpts,
		pool:    pool,
		metrics: &metrics.TreeMetrics{},
	}
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

func (mt *MultiTree) LoadVersion(version int64) error {
	for _, tree := range mt.Trees {
		if err := tree.LoadVersion(version); err != nil {
			return err
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
	}
	return mt.Hash(), version, nil
}

// Hash is a stand in for code at
// https://github.com/cosmos/cosmos-sdk/blob/80dd55f79bba8ab675610019a5764470a3e2fef9/store/types/commit_info.go#L30
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
