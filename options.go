package iavl

// Options define customized pruning/writing strategies for the IAVL tree
type Options struct {
	KeepEvery  int64
	KeepRecent int64
	Sync       bool
}

// DefaultOptions returns the default options for IAVL
func DefaultOptions() *Options {
	return &Options{
		KeepEvery:  1,
		KeepRecent: 0,
		Sync:       false,
	}
}

// PruningOptions returns Options with a given pruning strategy.
// Sync is set to false
func PruningOptions(keepEvery, keepRecent int64) *Options {
	return &Options{
		KeepEvery:  keepEvery,
		KeepRecent: keepRecent,
		Sync:       false,
	}
}

// BenchingOptions returns Options intended for benchmark tests
// with a given pruning strategy. Sync is set to true
func BenchingOptions(keepEvery, keepRecent int64) *Options {
	return &Options{
		KeepEvery:  keepEvery,
		KeepRecent: keepRecent,
		Sync:       true,
	}
}
