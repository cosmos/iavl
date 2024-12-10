package iavl

import "fmt"

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

func (tree *Tree) Export(version int64, order TraverseOrderType) (*Exporter, error) {
	ok, root := tree.getRecentRoot(version)
	if !ok {
		cloned, err := tree.ReadonlyClone()
		if err != nil {
			return nil, err
		}
		if err = cloned.LoadVersion(version); err != nil {
			return nil, err
		}
		root = cloned.root
	}
	exporter := &Exporter{
		tree:  tree,
		out:   make(chan *Node),
		errCh: make(chan error),
	}

	go func(traverseOrder TraverseOrderType) {
		defer close(exporter.out)
		if traverseOrder == PostOrder {
			exporter.postOrderNext(root)
		} else if traverseOrder == PreOrder {
			exporter.preOrderNext(root)
		}
	}(order)

	return exporter, nil
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

func (e *Exporter) Next() (*Node, error) {
	select {
	case node, ok := <-e.out:
		if !ok {
			return nil, ErrorExportDone
		}
		if node == nil {
			panic("nil node")
		}
		return node, nil
	case err := <-e.errCh:
		return nil, err
	}
}

func (e *Exporter) Close() error {
	return e.tree.Close()
}

var ErrorExportDone = fmt.Errorf("export done")
