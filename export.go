package iavl

import (
	"context"

	"github.com/pkg/errors"
)

// ExportDone is returned by Exporter.Next() when all items have been exported.
var ExportDone = errors.New("export is complete") // nolint:golint

// ExportNode contains exported node data.
type ExportNode struct {
	Key     []byte
	Value   []byte
	Version int64
	Height  int8
}

// Exporter exports nodes from an ImmutableTree. It is created by ImmutableTree.Export().
//
// Exported nodes can be imported into an empty tree with MutableTree.Import(). Nodes are exported
// depth-first post-order (LRN), this order must be preserved when importing in order to recreate
// the same tree structure.
type Exporter struct {
	tree   *ImmutableTree
	ch     chan *ExportNode
	cancel context.CancelFunc
}

// NewExporter creates a new Exporter. Callers must call Close() when done.
func newExporter(tree *ImmutableTree) *Exporter {
	ctx, cancel := context.WithCancel(context.Background())
	exporter := &Exporter{
		tree:   tree,
		ch:     make(chan *ExportNode, 64),
		cancel: cancel,
	}

	go exporter.export(ctx)

	return exporter
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

// Next fetches the next exported node, or returns ExportDone when done.
func (e *Exporter) Next() (*ExportNode, error) {
	exportNode, ok := <-e.ch
	if !ok {
		return nil, ExportDone
	}
	return exportNode, nil
}

// Close closes the exporter.
func (e *Exporter) Close() {
	e.cancel()
	for range e.ch { // drain channel
	}
}
