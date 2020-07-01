package iavl

// Options define tree options.
type Options struct {
	Sync bool
}

// DefaultOptions returns the default options for IAVL.
func DefaultOptions() *Options {
	return &Options{
		Sync: false,
	}
}
