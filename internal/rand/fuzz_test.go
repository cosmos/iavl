package common

import (
	mrand "math/rand"
	"testing"
)

func TestRandStrHuge(t *testing.T) {
	_ = RandStr(8455134228352958140)
}

func FuzzRandStr(f *testing.F) {
	// 1. Seed the fuzzer with lengths.
	mr := mrand.New(mrand.NewSource(10))
	for i := 0; i < 100; i++ {
		f.Add(mr.Int())
	}

	// 2. Now run the fuzzer.
	rr := NewRand()
	f.Fuzz(func(t *testing.T, n int) {
		s := rr.Str(n)
		// Ensure that we've got alphanumeric sequences entirely.
		for _, c := range s {
			switch {
			case c >= '0' && c <= '9':
			case c >= 'a' && c <= 'z':
			case c >= 'A' && c <= 'Z':
			default:
				t.Fatalf("found a non-alphanumeric value %c in %q", c, s)
			}
		}
	})
}
