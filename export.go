package iavl

import (
	"context"
	"io"
)

// ExportNode contains exported node data. Nodes must be exported in depth-first post-order.
type ExportNode struct {
	Key     []byte
	Value   []byte
	Version int64
	Height  int8
}

// Exporter exports data from an ImmutableTree.
type Exporter struct {
	tree   *ImmutableTree
	ch     chan *ExportNode
	cancel context.CancelFunc
}

// NewExporter creates a new Exporter. Callers must call Close() when done.
func NewExporter(tree *ImmutableTree) Exporter {
	ctx, cancel := context.WithCancel(context.Background())
	e := Exporter{
		tree:   tree,
		ch:     make(chan *ExportNode), // Should we use a buffered channel?
		cancel: cancel,
	}

	go e.export(ctx)

	return e
}

// export exports nodes
func (e *Exporter) export(ctx context.Context) {
	e.tree.root.traversePost(e.tree, true, func(node *Node) bool {
		select {
		case <-ctx.Done():
			return true
		default:
		}

		e.ch <- &ExportNode{
			Key:     node.key,
			Value:   node.value,
			Version: node.version,
			Height:  node.height,
		}
		return false
	})
	close(e.ch)
}

// Next fetches the next exported node, or returns io.EOF when done
func (e *Exporter) Next() (*ExportNode, error) {
	item, ok := <-e.ch
	if !ok {
		return nil, io.EOF
	}
	return item, nil
}

// Close closes the exporter
func (e *Exporter) Close() {
	e.cancel()
	for range e.ch {
	}
}
