package iavl

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/cosmos/iavl/v2/metrics"
	"github.com/cosmos/iavl/v2/testutil"
	"github.com/dustin/go-humanize"
)

// MultiTree encapsulates multiple IAVL trees, each with its own "store key" in the context of the Cosmos SDK.
// Within IAVL v2 is only used to test the IAVL v2 implementation, and for import/export of IAVL v2 state.
type MultiTree struct {
	logger Logger

	Trees map[string]*Tree

	pool             *NodePool
	rootPath         string
	treeOpts         TreeOptions
	shouldCheckpoint bool

	doneCh  chan saveVersionResult
	errorCh chan error
}

func NewMultiTree(logger Logger, rootPath string, opts TreeOptions) *MultiTree {
	if opts.MetricsProxy == nil {
		opts.MetricsProxy = metrics.NilMetrics{}
	}
	return &MultiTree{
		Trees:    make(map[string]*Tree),
		doneCh:   make(chan saveVersionResult, 1000),
		errorCh:  make(chan error, 1000),
		treeOpts: opts,
		pool:     NewNodePool(),
		rootPath: rootPath,
		logger:   logger,
	}
}

func ImportMultiTree(logger Logger, version int64, path string, treeOpts TreeOptions) (*MultiTree, error) {
	mt := NewMultiTree(logger, path, treeOpts)
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
		go func(p string) {
			tree, err := mt.newTree(p)
			if err != nil {
				errs <- err
			}
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
			logger.Info(fmt.Sprintf("imported %s", prefix))
			mt.Trees[prefix] = res.tree
		}
	}

	return mt, nil
}

func (mt *MultiTree) MountTree(storeKey string) error {
	dbPath := filepath.Join(mt.rootPath, storeKey)
	tree, err := mt.newTree(dbPath)
	if err != nil {
		return err
	}
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
		tree, err := mt.newTree(dbPath)
		if err != nil {
			return err
		}
		mt.Trees[prefix] = tree
	}
	return nil
}

func (mt *MultiTree) newTree(dbPath string) (*Tree, error) {
	pool := NewNodePool()
	opts := mt.treeOpts
	if _, ok := mt.treeOpts.MetricsProxy.(*metrics.StructMetrics); ok {
		opts.MetricsProxy = metrics.NewStructMetrics()
	}
	sql, err := NewSqliteDb(pool, defaultSqliteDbOptions(SqliteDbOptions{
		Path:    dbPath,
		Metrics: opts.MetricsProxy,
		Logger:  mt.logger,
	}))
	if err != nil {
		return nil, err
	}
	return NewTree(sql, pool, opts), nil
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
			mt.logger.Error("failed to save version", "error", err)
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
		// bz := workingBytes.Load()
		// sz := workingSize.Load()
		// fmt.Printf("version=%d work-bytes=%s work-size=%s mem-ceiling=%s\n",
		// 	version, humanize.IBytes(bz), humanize.Comma(sz), humanize.IBytes(mt.treeOpts.CheckpointMemory))
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
			mt.logger.Error("failed to snapshot", "error", err)
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
			mt.logger.Error("failed to warm leaves", "error", err)
			return err
		case <-mt.doneCh:
		}
	}
	return nil
}

func (mt *MultiTree) QueryReport(bins int) error {
	m := metrics.NewStructMetrics()
	for _, tree := range mt.Trees {
		sm, ok := tree.metricsProxy.(*metrics.StructMetrics)
		if !ok {
			continue
		}
		m.Add(sm)
		sm.SetQueryZero()
	}
	return m.QueryReport(bins)
}

func (mt *MultiTree) TestBuild(opts *testutil.TreeBuildOptions) (int64, error) {
	var (
		version  int64
		err      error
		cnt      = int64(1)
		memUsage = func() string {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			// For info on each, see: https://golang.org/pkg/runtime/#MemStats
			s := fmt.Sprintf("alloc=%s sys=%s gc=%d",
				humanize.Bytes(m.HeapAlloc),
				humanize.Bytes(m.Sys),
				m.NumGC)
			return s
		}
	)

	// generator
	itr := opts.Iterator
	fmt.Printf("Initial memory usage from generators:\n%s\n", memUsage())

	sampleRate := int64(100_000)
	if opts.SampleRate != 0 {
		sampleRate = opts.SampleRate
	}

	since := time.Now()
	itrStart := time.Now()

	report := func() error {
		dur := time.Since(since)

		var (
			workingBytes uint64
			workingSize  int64
			writeCount   int64
			writeTime    time.Duration
			hashCount    int64
		)
		for _, tr := range mt.Trees {
			sm, ok := tr.metricsProxy.(*metrics.StructMetrics)
			if !ok {
				return nil
			}
			workingBytes += tr.workingBytes
			workingSize += tr.workingSize
			writeTime += sm.WriteTime
			writeCount += sm.WriteLeaves + sm.WriteBranch
			hashCount += sm.TreeHash
			sm.WriteDurations = nil
			sm.WriteLeaves = 0
			sm.WriteBranch = 0
			sm.WriteTime = 0
			sm.TreeHash = 0
		}
		fmt.Printf("leaves=%s time=%s last=%s μ=%s version=%d work-size=%s work-bytes=%s %s\n",
			humanize.Comma(cnt),
			dur.Round(time.Millisecond),
			humanize.Comma(int64(float64(sampleRate)/time.Since(since).Seconds())),
			humanize.Comma(int64(float64(cnt)/time.Since(itrStart).Seconds())),
			version,
			humanize.Comma(workingSize),
			humanize.Bytes(workingBytes),
			memUsage())

		if writeTime > 0 {
			fmt.Printf("writes: cnt=%s wr/s=%s dur/wr=%s dur=%s hashes=%s\n",
				humanize.Comma(writeCount),
				humanize.Comma(int64(float64(writeCount)/writeTime.Seconds())),
				time.Duration(int64(writeTime)/writeCount),
				writeTime.Round(time.Millisecond),
				humanize.Comma(hashCount),
			)
		}

		if err := mt.QueryReport(0); err != nil {
			return fmt.Errorf("query report err: %w", err)
		}

		fmt.Println()
		since = time.Now()
		return nil
	}

	for ; itr.Valid(); err = itr.Next() {
		if err != nil {
			return cnt, err
		}
		changeset := itr.Nodes()
		for ; changeset.Valid(); err = changeset.Next() {
			cnt++
			if err != nil {
				return cnt, err
			}
			node := changeset.GetNode()
			key := node.Key

			tree, ok := mt.Trees[node.StoreKey]
			if !ok {
				if err := mt.MountTree(node.StoreKey); err != nil {
					return cnt, err
				}
				tree = mt.Trees[node.StoreKey]
			}

			if !node.Delete {
				_, err = tree.set(key, node.Value)
				if err != nil {
					return cnt, err
				}
			} else {
				_, _, err := tree.Remove(key)
				if err != nil {
					return cnt, err
				}
			}

			if cnt%sampleRate == 0 {
				if err := report(); err != nil {
					return cnt, err
				}
			}
		}

		_, version, err = mt.SaveVersionConcurrently()
		if err != nil {
			return cnt, err
		}
		if version%1000 == 0 {
			fmt.Printf("version: %d, hash: %x\n", version, mt.Hash())
		}
		if version == opts.Until {
			break
		}
	}
	fmt.Printf("final version: %d, hash: %x\n", version, mt.Hash())
	for sk, tree := range mt.Trees {
		fmt.Printf("storekey: %s height: %d, size: %d\n", sk, tree.Height(), tree.Size())
	}
	fmt.Printf("mean leaves/ms %s\n", humanize.Comma(cnt/time.Since(itrStart).Milliseconds()))
	if opts.UntilHash != "" && opts.UntilHash != fmt.Sprintf("%x", mt.Hash()) {
		return cnt, fmt.Errorf("invalid hash; expected %s, got %x", opts.UntilHash, mt.Hash())
	}
	return cnt, nil
}
