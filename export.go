package iavl

import (
	"bytes"
	"context"
	"io"
)

type ExportNode interface {
	FromNode(*Node) error
	ToNode() (*Node, error)
}

// ExportNodeNDB
type ExportNodeNDB struct {
	bytes []byte
}

func (e *ExportNodeNDB) FromNode(node *Node) error {
	var buf bytes.Buffer
	err := node.writeBytes(&buf)
	if err != nil {
		return err
	}
	e.bytes = buf.Bytes()
	return nil
}

func (e *ExportNodeNDB) ToNode() (*Node, error) {
	node, err := MakeNode(e.bytes)
	if err != nil {
		return nil, err
	}
	node._hash()
	return node, nil
}

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

		item := &ExportNodeNDB{}
		err := item.FromNode(node)
		if err != nil {
			panic(err) // FIXME
		}

		e.ch <- item
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
