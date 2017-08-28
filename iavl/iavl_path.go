package iavl

import (
	"bytes"

	"github.com/pkg/errors"
)

// PathToKey represents an inner path to a leaf node.
// Note that the nodes are ordered such that the last one is closest
// to the root of the tree.
type PathToKey struct {
	InnerNodes []IAVLProofInnerNode `json:"inner_nodes"`
}

func (p *PathToKey) String() string {
	str := ""
	for i := len(p.InnerNodes) - 1; i >= 0; i-- {
		str += p.InnerNodes[i].String() + "\n"
	}
	return str
}

// verify check that the leafNode's hash matches the path's LeafHash and that
// the root is the merkle hash of all the inner nodes.
func (p *PathToKey) verify(leafNode IAVLProofLeafNode, root []byte) error {
	leafHash := leafNode.Hash()
	for _, branch := range p.InnerNodes {
		leafHash = branch.Hash(leafHash)
	}
	if !bytes.Equal(root, leafHash) {
		return ErrInvalidPath()
	}
	return nil
}

func (p *PathToKey) isLeftmost() bool {
	for _, node := range p.InnerNodes {
		if len(node.Left) > 0 {
			return false
		}
	}
	return true
}

func (p *PathToKey) isRightmost() bool {
	for _, node := range p.InnerNodes {
		if len(node.Right) > 0 {
			return false
		}
	}
	return true
}

func (p *PathToKey) isEmpty() bool {
	return p == nil || len(p.InnerNodes) == 0
}

func (p *PathToKey) dropRoot() *PathToKey {
	if p.isEmpty() {
		return p
	}
	return &PathToKey{
		InnerNodes: p.InnerNodes[:len(p.InnerNodes)-1],
	}
}

func (p *PathToKey) hasCommonRoot(p2 *PathToKey) bool {
	if p.isEmpty() || p2.isEmpty() {
		return false
	}
	leftEnd := p.InnerNodes[len(p.InnerNodes)-1]
	rightEnd := p2.InnerNodes[len(p2.InnerNodes)-1]

	return bytes.Equal(leftEnd.Left, rightEnd.Left) &&
		bytes.Equal(leftEnd.Right, rightEnd.Right)
}

func (p *PathToKey) isLeftAdjacentTo(p2 *PathToKey) bool {
	for p.hasCommonRoot(p2) {
		p, p2 = p.dropRoot(), p2.dropRoot()
	}
	p, p2 = p.dropRoot(), p2.dropRoot()

	return p.isRightmost() && p2.isLeftmost()
}

// PathWithNode is a path to a key which includes the leaf node at that key.
type PathWithNode struct {
	Path *PathToKey        `json:"path"`
	Node IAVLProofLeafNode `json:"node"`
}

func (p *PathWithNode) verify(root []byte) error {
	return p.Path.verify(p.Node, root)
}

// verifyPaths verifies the left and right paths individually, and makes sure
// the ordering is such that left < startKey <= endKey < right.
func verifyPaths(left, right *PathWithNode, startKey, endKey, root []byte) error {
	if bytes.Compare(startKey, endKey) == 1 {
		return ErrInvalidInputs
	}
	if left != nil {
		if err := left.verify(root); err != nil {
			return ErrInvalidPath()
		}
		if !left.Node.isLesserThan(startKey) {
			return ErrInvalidPath()
		}
	}
	if right != nil {
		if err := right.verify(root); err != nil {
			return ErrInvalidPath()
		}
		if !right.Node.isGreaterThan(endKey) {
			return ErrInvalidPath()
		}
	}
	return nil
}

// Checks that all paths are adjacent to one another, ie. that there are no
// keys missing.
func verifyNoMissingKeys(paths []*PathToKey) error {
	ps := make([]*PathToKey, 0, len(paths))
	for _, p := range paths {
		if p != nil {
			ps = append(ps, p)
		}
	}
	for i := 0; i < len(ps)-1; i++ {
		// Always check from left to right, since paths are always in ascending order.
		if !ps[i].isLeftAdjacentTo(ps[i+1]) {
			return errors.Errorf("paths #%d and #%d are not adjacent", i, i+1)
		}
	}
	return nil
}

// Checks that with the given left and right paths, no keys can exist in between.
// Supports nil paths to signify out-of-range.
func verifyKeyAbsence(left, right *PathWithNode) error {
	if left != nil && left.Path.isRightmost() {
		// Range starts outside of the right boundary.
		return nil
	} else if right != nil && right.Path.isLeftmost() {
		// Range ends outside of the left boundary.
		return nil
	} else if left != nil && right != nil &&
		left.Path.isLeftAdjacentTo(right.Path) {
		// Range is between two existing keys.
		return nil
	}
	return ErrInvalidProof()
}
