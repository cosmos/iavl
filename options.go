package iavl

// Define Options for customizing pruning/writing strategy of IAVL tree
type Options struct {
	KeepEvery  int64
	KeepRecent int64
	Sync       bool
}

// Returns the default options for IAVL
func DefaultOptions() *Options {
	return &Options{
		KeepEvery:  1,
		KeepRecent: 0,
		Sync:       false,
	}
}

// Return Options with given pruning strategy. Sync=false
func PruningOptions(keepEvery, keepRecent int64) *Options {
	return &Options{
		KeepEvery:  keepEvery,
		KeepRecent: keepRecent,
		Sync:       false,
	}
}

// Return Options intended for benchmark tests
// with given pruning strategy. Sync = true
func BenchingOptions(keepEvery, keepRecent int64) *Options {
	return &Options{
		KeepEvery:  keepEvery,
		KeepRecent: keepRecent,
		Sync:       true,
	}
}
