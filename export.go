package iavl

import (
	"context"
	"io"
)

// ExportedNode represents an exported node
type ExportedNode struct {
	Key []byte
}

// Exporter exports data from an ImmutableTree
type Exporter struct {
	tree   *ImmutableTree
	ch     chan ExportedNode
	cancel context.CancelFunc
}

// NewExporter creates a new Exporter. Callers must call Close() when done.
func NewExporter(ctx context.Context, tree *ImmutableTree) Exporter {
	ctx, cancel := context.WithCancel(ctx)
	e := Exporter{
		tree:   tree,
		ch:     make(chan ExportedNode), // Should we use a buffered channel?
		cancel: cancel,
	}

	go e.traverse(ctx)

	return e
}

// traverse traverses the tree and puts nodes in the queue
func (e *Exporter) traverse(ctx context.Context) {
	e.tree.root.traverse(e.tree, true, func(node *Node) bool {
		select {
		case <-ctx.Done():
			return true
		default:
		}
		e.ch <- ExportedNode{
			Key: node.key,
		}
		return false
	})
	close(e.ch)
}

// Next fetches the next exported node, or returns io.EOF when done
func (e *Exporter) Next() (ExportedNode, error) {
	if item, ok := <-e.ch; ok {
		return item, nil
	} else {
		return ExportedNode{}, io.EOF
	}
}

// Close closes the exporter
func (e *Exporter) Close() {
	e.cancel()
	for range e.ch {
	}
}
