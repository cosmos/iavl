package iavl

import (
	"bytes"
	"context"
	"io"
)

type ExportNode *ExportNodeMinimal

// ExportNodeMinimal stores the minimum amount of data
type ExportNodeMinimal struct {
	Key     []byte
	Value   []byte
	Version int64
	Height  int8
}

// ExportNodeNDB stores data in the same format as the internal NodeDB database
type ExportNodeNDB []byte

// Exporter exports data from an ImmutableTree
type Exporter struct {
	tree   *ImmutableTree
	ch     chan ExportNode
	cancel context.CancelFunc
}

// NewExporter creates a new Exporter. Callers must call Close() when done.
func NewExporter(tree *ImmutableTree) Exporter {
	ctx, cancel := context.WithCancel(context.Background())
	e := Exporter{
		tree:   tree,
		ch:     make(chan ExportNode), // Should we use a buffered channel?
		cancel: cancel,
	}

	go e.exportMinimal(ctx)
	//go e.exportNDB(ctx)

	return e
}

// exportMinimal exports minimal nodes
func (e *Exporter) exportMinimal(ctx context.Context) {
	e.tree.root.traverseAfter(e.tree, true, func(node *Node) bool {
		select {
		case <-ctx.Done():
			return true
		default:
		}

		e.ch <- &ExportNodeMinimal{
			Key:     node.key,
			Value:   node.value,
			Version: node.version,
			Height:  node.height,
		}
		return false
	})
	close(e.ch)
}

// exportNDB exports nodes as NDB byte buffers
func (e *Exporter) exportNDB(ctx context.Context) {
	e.tree.root.traverse(e.tree, true, func(node *Node) bool {
		select {
		case <-ctx.Done():
			return true
		default:
		}

		var buf bytes.Buffer
		err := node.writeBytes(&buf)
		if err != nil {
			panic(err) // FIXME
		}

		//e.ch <- ExportNode(buf.Bytes())
		return false
	})
	close(e.ch)
}

// Next fetches the next exported node, or returns io.EOF when done
func (e *Exporter) Next() (ExportNode, error) {
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
