package iavl

import (
	"testing"

	"github.com/tendermint/tendermint/libs/db"
)

// Generate many programs and run them.
func TestMutableTreeFuzz(t *testing.T) {
	maxIterations := testFuzzIterations
	progsPerIteration := 100000
	iterations := 0

	for size := 5; iterations < maxIterations; size++ {
		for i := 0; i < progsPerIteration/size; i++ {
			tree := NewMutableTree(db.NewMemDB(), 0)
			Program := genRandomProgram(size)
			err := Program.Execute(tree)
			if err != nil {
				t.Fatalf("Error after %d iterations (size %d): %s\n%s", iterations, size, err.Error(), tree.String())
			}
			iterations++
		}
	}
}
