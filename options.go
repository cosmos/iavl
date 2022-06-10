package iavl

import "sync/atomic"

// Statisc about db runtime state
type Statistics struct {
	// Each time GetNode and GetFastNode operation hit cache
	cacheHitCnt uint64

	// Each time GetNode and GetFastNode operation miss cache
	cacheMissCnt uint64
}

func (stat *Statistics) IncCacheHitCnt() {
	atomic.AddUint64(&stat.cacheHitCnt, 1)
}

func (stat *Statistics) IncCacheMissCnt() {
	atomic.AddUint64(&stat.cacheMissCnt, 1)
}

func (stat *Statistics) GetCacheHitCnt() uint64 {
	return atomic.LoadUint64(&stat.cacheHitCnt)
}

func (stat *Statistics) GetCacheMissCnt() uint64 {
	return atomic.LoadUint64(&stat.cacheMissCnt)
}

func (stat *Statistics) Reset() {
	atomic.StoreUint64(&stat.cacheHitCnt, 0)
	atomic.StoreUint64(&stat.cacheMissCnt, 0)
}

// Options define tree options.
type Options struct {
	// Sync synchronously flushes all writes to storage, using e.g. the fsync syscall.
	// Disabling this significantly improves performance, but can lose data on e.g. power loss.
	Sync bool

	// InitialVersion specifies the initial version number. If any versions already exist below
	// this, an error is returned when loading the tree. Only used for the initial SaveVersion()
	// call.
	InitialVersion uint64

	// When Stat is not nil, statistical logic needs to be executed
	Stat *Statistics
}

// DefaultOptions returns the default options for IAVL.
func DefaultOptions() Options {
	return Options{}
}
