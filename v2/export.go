package iavl

import (
	"errors"
)

// TraverseOrderType is the type of the order in which the tree is traversed.
type TraverseOrderType uint8

const (
	PreOrder TraverseOrderType = iota
	PostOrder
)

type Exporter struct {
	tree  *Tree
	out   chan *Node
	errCh chan error
}

func (tree *Tree) Export(order TraverseOrderType) *Exporter {
	exporter := &Exporter{
		tree:  tree,
		out:   make(chan *Node),
		errCh: make(chan error),
	}

	go func(traverseOrder TraverseOrderType) {
		defer close(exporter.out)
		defer close(exporter.errCh)

		if traverseOrder == PostOrder {
			exporter.postOrderNext(tree.root)
		} else if traverseOrder == PreOrder {
			exporter.preOrderNext(tree.root)
		}
	}(order)

	return exporter
}

func (e *Exporter) postOrderNext(node *Node) {
	if node.isLeaf() {
		e.out <- node
		return
	}

	left, err := node.getLeftNode(e.tree)
	if err != nil {
		e.errCh <- err
		return
	}
	e.postOrderNext(left)

	right, err := node.getRightNode(e.tree)
	if err != nil {
		e.errCh <- err
		return
	}
	e.postOrderNext(right)

	e.out <- node
}

func (e *Exporter) preOrderNext(node *Node) {
	e.out <- node
	if node.isLeaf() {
		return
	}

	left, err := node.getLeftNode(e.tree)
	if err != nil {
		e.errCh <- err
		return
	}
	e.preOrderNext(left)

	right, err := node.getRightNode(e.tree)
	if err != nil {
		e.errCh <- err
		return
	}
	e.preOrderNext(right)
}

func (e *Exporter) Next() (*SnapshotNode, error) {
	select {
	case node, ok := <-e.out:
		if !ok {
			return nil, ErrorExportDone
		}
		return &SnapshotNode{
			Key:     node.key,
			Value:   node.value,
			Version: node.nodeKey.Version(),
			Height:  node.subtreeHeight,
		}, nil
	case err := <-e.errCh:
		return nil, err
	}
}

var ErrorExportDone = errors.New("export done")
