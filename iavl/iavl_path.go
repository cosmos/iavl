package iavl

import (
	"bytes"
	"fmt"

	"github.com/pkg/errors"
	"github.com/tendermint/go-wire/data"
)

// PathToKey represents an inner path to a leaf node.
// Note that the nodes are ordered such that the last one is closest
// to the root of the tree.
type PathToKey struct {
	LeafHash   data.Bytes           `json:"leaf_hash"`
	InnerNodes []IAVLProofInnerNode `json:"inner_nodes"`
}

func (p *PathToKey) String() string {
	str := ""
	for i := len(p.InnerNodes) - 1; i >= 0; i-- {
		str += p.InnerNodes[i].String() + "\n"
	}
	str += fmt.Sprintf("hash(leaf)=%s\n", p.LeafHash)
	return str
}

func (p *PathToKey) verify(leafNode IAVLProofLeafNode, root []byte) error {
	leafHash := leafNode.Hash()
	if !bytes.Equal(leafHash, p.LeafHash) {
		return ErrInvalidPath
	}
	hash := leafHash
	for _, branch := range p.InnerNodes {
		hash = branch.Hash(hash)
	}
	if !bytes.Equal(root, hash) {
		return ErrInvalidPath
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
		LeafHash:   p.LeafHash,
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
// the ordering is such that left < startKey/endKey < right.
func verifyPaths(left, right *PathWithNode, startKey, endKey, root []byte) error {
	if left != nil {
		if err := left.verify(root); err != nil {
			return ErrInvalidPath
		}
		if !left.Node.isLesserThan(startKey) {
			return ErrInvalidPath
		}
	}
	if right != nil {
		if err := right.verify(root); err != nil {
			return ErrInvalidPath
		}
		if !right.Node.isGreaterThan(endKey) {
			return ErrInvalidPath
		}
	}
	if left != nil && right != nil {
		if !left.Node.isLesserThan(right.Node.KeyBytes) {
			return ErrInvalidPath
		}
	}
	return nil
}

// Checks that all paths are adjacent to one another, ie. that there are no
// keys missing.
func verifyPathAdjacency(paths []*PathToKey) error {
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
