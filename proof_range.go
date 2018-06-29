package iavl

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/tendermint/iavl/sha256truncated"
	cmn "github.com/tendermint/tmlibs/common"
)

type RangeProof struct {
	// You don't need the right path because
	// it can be derived from what we have.
	RootHash   cmn.HexBytes    `json:"root_hash"`
	LeftPath   PathToLeaf      `json:"left_path"`
	InnerNodes []PathToLeaf    `json:"inner_nodes"`
	Leaves     []proofLeafNode `json:"leaves"`
	// temporary
	treeEnd int // 0 if not set, 1 if true, -1 if false.
}

// String returns a string representation of the proof.
func (proof *RangeProof) String() string {
	return proof.StringIndented("")
}

func (proof *RangeProof) StringIndented(indent string) string {
	istrs := make([]string, 0, len(proof.InnerNodes))
	for _, ptl := range proof.InnerNodes {
		istrs = append(istrs, ptl.StringIndented(indent+"    "))
	}
	lstrs := make([]string, 0, len(proof.Leaves))
	for _, leaf := range proof.Leaves {
		lstrs = append(lstrs, leaf.StringIndented(indent+"    "))
	}
	return fmt.Sprintf(`RangeProof{
%s  RootHash: %X
%s  LeftPath: %v
%s  InnerNodes:
%s    %v
%s  Leaves:
%s    %v
%s  (treeEnd): %v
%s}`,
		indent, proof.RootHash,
		indent, proof.LeftPath.StringIndented(indent+"  "),
		indent,
		indent, strings.Join(istrs, "\n"+indent+"    "),
		indent,
		indent, strings.Join(lstrs, "\n"+indent+"    "),
		indent, proof.treeEnd,
		indent)
}

// Verify that a leaf is some value.
// Does not assume that the proof itself is value.
// For that, use Verify(root).
func (proof *RangeProof) VerifyItem(i int, key, value []byte) error {
	if proof == nil {
		return cmn.ErrorWrap(ErrInvalidProof, "proof is nil")
	}
	if !bytes.Equal(proof.Leaves[i].Key, key) {
		return cmn.ErrorWrap(ErrInvalidProof, "leaf key not same")
	}
	valueHash := sha256truncated.Hash(value)
	if !bytes.Equal(proof.Leaves[i].ValueHash, valueHash) {
		return cmn.ErrorWrap(ErrInvalidProof, "leaf value hash not same")
	}
	return nil
}

// Verify that proof is valid absence proof for key.
// Does not assume that the proof itself is valid.
// For that, use Verify(root).
func (proof *RangeProof) VerifyAbsence(key []byte) error {
	if proof == nil {
		return cmn.ErrorWrap(ErrInvalidProof, "proof is nil")
	}
	if proof.treeEnd == 0 {
		return cmn.NewError("must call Verify(root) first.")
	}
	cmp := bytes.Compare(key, proof.Leaves[0].Key)
	if cmp < 0 {
		if proof.LeftPath.isLeftmost() {
			return nil
		} else {
			return cmn.NewError("absence not proved by left path")
		}
	} else if cmp == 0 {
		return cmn.NewError("absence disproved via first item #0")
	}
	if len(proof.LeftPath) == 0 {
		return nil // proof ok
	}
	if proof.LeftPath.isRightmost() {
		return nil
	}

	// See if any of the leaves are greater than key.
	for i := 1; i < len(proof.Leaves); i++ {
		leaf := proof.Leaves[i]
		cmp := bytes.Compare(key, leaf.Key)
		if cmp < 0 {
			return nil // proof ok
		} else if cmp == 0 {
			return cmn.NewError("absence disproved via item #%v", i)
		} else {
			if i == len(proof.Leaves)-1 {
				// If last item, check whether
				// it's the last item in teh tree.

			}
			continue
		}
	}

	// It's still a valid proof if our last leaf is the rightmost child.
	if proof.treeEnd == 1 {
		return nil // OK!
	}

	// It's not a valid absence proof.
	if len(proof.Leaves) < 2 {
		return cmn.NewError("absence not proved by right leaf (need another leaf?)")
	} else {
		return cmn.NewError("absence not proved by right leaf")
	}
}

// Verify that proof is valid.
func (proof *RangeProof) Verify(root []byte) error {
	if proof == nil {
		return cmn.ErrorWrap(ErrInvalidProof, "proof is nil")
	}
	treeEnd, err := proof._verify(root)
	if err == nil {
		if treeEnd {
			proof.treeEnd = 1 // memoize
		} else {
			proof.treeEnd = -1 // memoize
		}
	}
	return err
}

func (proof *RangeProof) _verify(root []byte) (treeEnd bool, err error) {
	if !bytes.Equal(proof.RootHash, root) {
		return false, cmn.ErrorWrap(ErrInvalidRoot, "root hash doesn't match")
	}
	if len(proof.Leaves) == 0 {
		return false, cmn.ErrorWrap(ErrInvalidProof, "no leaves")
	}
	if len(proof.InnerNodes)+1 != len(proof.Leaves) {
		return false, cmn.ErrorWrap(ErrInvalidProof, "InnerNodes vs Leaves length mismatch, leaves should be 1 more.")
	}

	// Start from the left path and prove each leaf.

	// shared across recursive calls
	var leaves = proof.Leaves
	var innersq = proof.InnerNodes
	var VERIFY func(path PathToLeaf, root []byte, rightmost bool) (treeEnd bool, done bool, err error)
	// rightmost: is the root a rightmost child of the tree?
	// treeEnd: true iff the last leaf is the last item of the tree.
	// NOTE: root doesn't necessarily mean root of the tree here.
	VERIFY = func(path PathToLeaf, root []byte, rightmost bool) (treeEnd bool, done bool, err error) {

		// Pop next leaf.
		nleaf, rleaves := leaves[0], leaves[1:]
		leaves = rleaves

		// Verify leaf with path.
		if err := (pathWithLeaf{
			Path: path,
			Leaf: nleaf,
		}).verify(root); err != nil {
			return false, false, err.Trace(0, "verifying some path to a leaf")
		}

		// If we don't have any leaves left, we're done.
		if len(leaves) == 0 {
			rightmost = rightmost && path.isRightmost()
			return rightmost, true, nil
		}

		// Prove along path (until we run out of leaves).
		for len(path) > 0 {

			// Drop the leaf-most (last-most) inner nodes from path
			// until we encounter one with a left hash.
			// We assume that the left side is already verified.
			// rpath: rest of path
			// lpath: last path item
			rpath, lpath := path[:len(path)-1], path[len(path)-1]
			path = rpath
			if len(lpath.Right) == 0 {
				continue
			}

			// Pop next inners, a PathToLeaf (e.g. []proofInnerNode).
			inners, rinnersq := innersq[0], innersq[1:]
			innersq = rinnersq

			// Recursively verify inners against remaining leaves.
			treeEnd, done, err := VERIFY(inners, lpath.Right, rightmost && rpath.isRightmost())
			if err != nil {
				return treeEnd, false, cmn.ErrorWrap(err, "recursive VERIFY call")
			} else if done {
				return treeEnd, true, nil
			}
		}

		// We're not done yet. No error, not done either.  Technically if
		// rightmost, we know there's an error "left over leaves -- malformed
		// proof", but we return that at the top level, below.
		return false, false, nil
	}

	// Verify!
	path := proof.LeftPath
	treeEnd, done, err := VERIFY(path, root, true)
	if err != nil {
		return treeEnd, cmn.ErrorWrap(err, "root VERIFY call")
	} else if !done {
		return treeEnd, cmn.ErrorWrap(ErrInvalidProof, "left over leaves -- malformed proof")
	}

	// Ok!
	return treeEnd, nil
}

///////////////////////////////////////////////////////////////////////////////

// keyStart is inclusive and keyEnd is exclusive.
// If keyStart or keyEnd don't exist, the leaf before keyStart
// or after keyEnd will also be included, but not be included in values.
// If keyEnd-1 exists, no later leaves will be included.
// Limit is never exceeded.
func (t *Tree) getRangeProof(keyStart, keyEnd []byte, limit int) (proof *RangeProof, values [][]byte, err error) {
	if limit < 0 {
		panic("limit must be greater or equal to 0 -- 0 means no limit")
	}
	if t.root == nil {
		return nil, nil, cmn.ErrorWrap(ErrNilRoot, "")
	}
	t.root.hashWithCount() // Ensure that all hashes are calculated.

	// Get the first key/value pair proof, which provides us with the left key.
	path, left, err := t.root.PathToLeaf(t, keyStart)
	if err != nil {
		// Key doesn't exist, but instead we got the prev leaf (or the
		// first leaf), which provides proof of absence).
		err = nil
	}
	values = append(values, left.value)
	var leaves = []proofLeafNode{proofLeafNode{
		Key:       left.key,
		ValueHash: sha256truncated.Hash(left.value),
		Version:   left.version,
	}}

	// 1: Special case if limit is 1.
	// 2: Special case if keyEnd is left.key+1.
	_stop := false
	if limit == 1 {
		_stop = true // case 1
	} else if keyEnd != nil && bytes.Compare(cpIncr(left.key), keyEnd) >= 0 {
		_stop = true // case 2
	}
	if _stop {
		return &RangeProof{
			RootHash: t.root.hash,
			LeftPath: path,
			Leaves:   leaves,
		}, values, nil
	}

	if keyEnd != nil && bytes.Compare(cpIncr(left.key), keyEnd) >= 0 {
		return &RangeProof{
			RootHash: t.root.hash,
			LeftPath: path,
			Leaves:   leaves,
		}, values, nil
	}

	// Get the key after left.key to iterate from.
	afterLeft := cpIncr(left.key)

	// Traverse starting from afterLeft, until keyEnd or the next leaf
	// after keyEnd.
	// nolint
	var innersq = []PathToLeaf(nil)
	var inners = PathToLeaf(nil)
	var lastDepth uint8 = 0
	var leafCount = 1 // from left above.
	var pathCount = 0
	// var values [][]byte defined as function outs.

	t.root.traverseInRange(t, afterLeft, nil, true, false, 0,
		func(node *Node, depth uint8) (stop bool) {

			// Track when we diverge from path, or when we've exhausted path,
			// since the first innersq shouldn't include it.
			if pathCount != -1 {
				if len(path) <= pathCount {
					// We're done with path counting.
					pathCount = -1
				} else {
					pn := path[pathCount]
					if pn.Height != node.height ||
						pn.Left != nil && !bytes.Equal(pn.Left, node.leftHash) ||
						pn.Right != nil && !bytes.Equal(pn.Right, node.rightHash) {

						// We've diverged, so start appending to inners.
						pathCount = -1
					} else {
						pathCount += 1
					}
				}
			}

			if node.height == 0 {
				// Leaf node.
				// Append inners to innersq.
				innersq = append(innersq, inners)
				inners = PathToLeaf(nil)
				// Append leaf to leaves.
				leaves = append(leaves, proofLeafNode{
					Key:       node.key,
					ValueHash: sha256truncated.Hash(node.value),
					Version:   node.version,
				})
				// Append value to values.
				values = append(values, node.value)
				leafCount += 1
				// Maybe terminate because we found enough leaves.
				if limit > 0 && limit <= leafCount {
					return true
				}
				// Maybe terminate because we've found keyEnd-1 or after.
				if keyEnd != nil && bytes.Compare(cpIncr(node.key), keyEnd) >= 0 {
					return true
				}
			} else {
				// Inner node.
				if pathCount >= 0 {
					// Skip redundant path items.
				} else {
					inners = append(inners, proofInnerNode{
						Height:  node.height,
						Size:    node.size,
						Version: node.version,
						Left:    nil, // left is nil for range proof inners
						Right:   node.rightHash,
					})
				}
			}
			lastDepth = depth
			return false
		},
	)

	return &RangeProof{
		RootHash:   t.root.hash,
		LeftPath:   path,
		InnerNodes: innersq,
		Leaves:     leaves,
	}, values, nil
}

//----------------------------------------

// GetWithProof gets the value under the key if it exists, or returns nil.
// A proof of existence or absence is returned alongside the value.
func (t *Tree) GetWithProof(key []byte) (value []byte, proof *RangeProof, err error) {
	proof, values, err := t.getRangeProof(key, cpIncr(key), 2)
	if err == nil {
		if len(values) > 0 {
			if !bytes.Equal(proof.Leaves[0].Key, key) {
				return nil, proof, nil
			} else {
				return values[0], proof, nil
			}
		} else {
			return nil, proof, nil
		}
	}
	return nil, nil, cmn.ErrorWrap(err, "could not construct any proof")
}

// GetRangeWithProof gets key/value pairs within the specified range and limit.
// To specify a descending range, swap the start and end keys.
func (t *Tree) GetRangeWithProof(startKey []byte, endKey []byte, limit int) (keys, values [][]byte, proof *RangeProof, err error) {
	proof, values, err = t.getRangeProof(startKey, endKey, limit)
	for _, leaf := range proof.Leaves {
		keys = append(keys, leaf.Key)
	}
	return
}

// GetVersionedWithProof gets the value under the key at the specified version
// if it exists, or returns nil.  A proof of existence or absence is returned
// alongside the value.
func (tree *VersionedTree) GetVersionedWithProof(key []byte, version int64) ([]byte, *RangeProof, error) {
	if t, ok := tree.versions[version]; ok {
		return t.GetWithProof(key)
	}
	return nil, nil, cmn.ErrorWrap(ErrVersionDoesNotExist, "")
}

// GetVersionedRangeWithProof gets key/value pairs within the specified range
// and limit. To specify a descending range, swap the start and end keys.
//
// Returns a list of values, a list of keys, and a proof.
func (tree *VersionedTree) GetVersionedRangeWithProof(startKey, endKey []byte, limit int, version int64) ([][]byte, [][]byte, *RangeProof, error) {
	if t, ok := tree.versions[version]; ok {
		return t.GetRangeWithProof(startKey, endKey, limit)
	}
	return nil, nil, nil, cmn.ErrorWrap(ErrVersionDoesNotExist, "")
}
