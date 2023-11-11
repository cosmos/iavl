package iavl

import "fmt"

type Exporter struct {
	tree  *Tree
	out   chan *Node
	errCh chan error
}

func (tree *Tree) ExportPreOrder() *Exporter {
	exporter := &Exporter{
		tree:  tree,
		out:   make(chan *Node),
		errCh: make(chan error),
	}
	go func() {
		defer close(exporter.out)
		defer close(exporter.errCh)
		exporter.preOrderNext(tree.root)
	}()
	return exporter
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

var ErrorExportDone = fmt.Errorf("export done")
