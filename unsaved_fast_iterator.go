package iavl

import (
	"bytes"
	"errors"
	"sort"

	dbm "github.com/cosmos/cosmos-db"
	"github.com/cosmos/iavl/fastnode"
)

var (
	errUnsavedFastIteratorNilAdditionsGiven = errors.New("unsaved fast iterator must be created with unsaved additions but they were nil")

	errUnsavedFastIteratorNilRemovalsGiven = errors.New("unsaved fast iterator must be created with unsaved removals but they were nil")
)

// UnsavedFastIterator is a dbm.Iterator for ImmutableTree
// it iterates over the latest state via fast nodes,
// taking advantage of keys being located in sequence in the underlying database.
type UnsavedFastIterator struct {
	start, end   []byte
	valid        bool
	ascending    bool
	err          error
	ndb          *nodeDB
	nextKey      []byte
	nextVal      []byte
	fastIterator dbm.Iterator

	nextUnsavedNodeIdx       int
	unsavedFastNodeAdditions map[string]*fastnode.Node
	unsavedFastNodeRemovals  map[string]interface{}
	unsavedFastNodesToSort   [][]byte
}

var _ dbm.Iterator = (*UnsavedFastIterator)(nil)

func NewUnsavedFastIterator(start, end []byte, ascending bool, ndb *nodeDB, unsavedFastNodeAdditions map[string]*fastnode.Node, unsavedFastNodeRemovals map[string]interface{}) *UnsavedFastIterator {
	iter := &UnsavedFastIterator{
		start:                    start,
		end:                      end,
		ascending:                ascending,
		ndb:                      ndb,
		unsavedFastNodeAdditions: unsavedFastNodeAdditions,
		unsavedFastNodeRemovals:  unsavedFastNodeRemovals,
		nextKey:                  nil,
		nextVal:                  nil,
		nextUnsavedNodeIdx:       0,
		fastIterator:             NewFastIterator(start, end, ascending, ndb),
	}

	// We need to ensure that we iterate over saved and unsaved state in order.
	// The strategy is to sort unsaved nodes, the fast node on disk are already sorted.
	// Then, we keep a pointer to both the unsaved and saved nodes, and iterate over them in order efficiently.
	for _, fastNode := range unsavedFastNodeAdditions {
		if start != nil && bytes.Compare(fastNode.GetKey(), start) < 0 {
			continue
		}

		if end != nil && bytes.Compare(fastNode.GetKey(), end) >= 0 {
			continue
		}

		iter.unsavedFastNodesToSort = append(iter.unsavedFastNodesToSort, fastNode.GetKey())
	}

	sort.Slice(iter.unsavedFastNodesToSort, func(i, j int) bool {
		cmp := bytes.Compare(iter.unsavedFastNodesToSort[i], iter.unsavedFastNodesToSort[j])
		if ascending {
			return cmp < 0
		}
		return cmp > 0
	})

	if iter.ndb == nil {
		iter.err = errFastIteratorNilNdbGiven
		iter.valid = false
		return iter
	}

	if iter.unsavedFastNodeAdditions == nil {
		iter.err = errUnsavedFastIteratorNilAdditionsGiven
		iter.valid = false
		return iter
	}

	if iter.unsavedFastNodeRemovals == nil {
		iter.err = errUnsavedFastIteratorNilRemovalsGiven
		iter.valid = false
		return iter
	}

	// Move to the first elemenet
	iter.Next()

	return iter
}

// Domain implements dbm.Iterator.
// Maps the underlying nodedb iterator domain, to the 'logical' keys involved.
func (iter *UnsavedFastIterator) Domain() ([]byte, []byte) {
	return iter.start, iter.end
}

// Valid implements dbm.Iterator.
func (iter *UnsavedFastIterator) Valid() bool {
	if iter.start != nil && iter.end != nil {
		if bytes.Compare(iter.end, iter.start) != 1 {
			return false
		}
	}

	return iter.fastIterator.Valid() || iter.nextUnsavedNodeIdx < len(iter.unsavedFastNodesToSort) || (iter.nextKey != nil && iter.nextVal != nil)
}

// Key implements dbm.Iterator
func (iter *UnsavedFastIterator) Key() []byte {
	return iter.nextKey
}

// Value implements dbm.Iterator
func (iter *UnsavedFastIterator) Value() []byte {
	return iter.nextVal
}

// Next implements dbm.Iterator
// Its effectively running the constant space overhead algorithm for streaming through sorted lists:
// the sorted lists being underlying fast nodes & unsavedFastNodeChanges
func (iter *UnsavedFastIterator) Next() {
	if iter.ndb == nil {
		iter.err = errFastIteratorNilNdbGiven
		iter.valid = false
		return
	}

	diskKeyStr := iter.fastIterator.Key()
	if iter.fastIterator.Valid() && iter.nextUnsavedNodeIdx < len(iter.unsavedFastNodesToSort) {

		if iter.unsavedFastNodeRemovals[string(diskKeyStr)] != nil {
			// If next fast node from disk is to be removed, skip it.
			iter.fastIterator.Next()
			iter.Next()
			return
		}

		nextUnsavedKey := iter.unsavedFastNodesToSort[iter.nextUnsavedNodeIdx]
		nextUnsavedNode := iter.unsavedFastNodeAdditions[string(nextUnsavedKey)]

		var isUnsavedNext bool
		cmp := bytes.Compare(diskKeyStr, nextUnsavedKey)
		if iter.ascending {
			isUnsavedNext = cmp >= 0
		} else {
			isUnsavedNext = cmp <= 0
		}

		if isUnsavedNext {
			// Unsaved node is next

			if cmp == 0 {
				// Unsaved update prevails over saved copy so we skip the copy from disk
				iter.fastIterator.Next()
			}

			iter.nextKey = nextUnsavedNode.GetKey()
			iter.nextVal = nextUnsavedNode.GetValue()

			iter.nextUnsavedNodeIdx++
			return
		}
		// Disk node is next
		iter.nextKey = iter.fastIterator.Key()
		iter.nextVal = iter.fastIterator.Value()

		iter.fastIterator.Next()
		return
	}

	// if only nodes on disk are left, we return them
	if iter.fastIterator.Valid() {
		if iter.unsavedFastNodeRemovals[string(diskKeyStr)] != nil {
			// If next fast node from disk is to be removed, skip it.
			iter.fastIterator.Next()
			iter.Next()
			return
		}

		iter.nextKey = iter.fastIterator.Key()
		iter.nextVal = iter.fastIterator.Value()

		iter.fastIterator.Next()
		return
	}

	// if only unsaved nodes are left, we can just iterate
	if iter.nextUnsavedNodeIdx < len(iter.unsavedFastNodesToSort) {
		nextUnsavedKey := iter.unsavedFastNodesToSort[iter.nextUnsavedNodeIdx]
		nextUnsavedNode := iter.unsavedFastNodeAdditions[string(nextUnsavedKey)]

		iter.nextKey = nextUnsavedNode.GetKey()
		iter.nextVal = nextUnsavedNode.GetValue()

		iter.nextUnsavedNodeIdx++
		return
	}

	iter.nextKey = nil
	iter.nextVal = nil
}

// Close implements dbm.Iterator
func (iter *UnsavedFastIterator) Close() error {
	iter.valid = false
	return iter.fastIterator.Close()
}

// Error implements dbm.Iterator
func (iter *UnsavedFastIterator) Error() error {
	return iter.err
}
