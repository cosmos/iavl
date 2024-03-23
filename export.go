package iavl

import (
	"bytes"
	"context"
	"errors"
	"fmt"
)

// exportBufferSize is the number of nodes to buffer in the exporter. It improves throughput by
// processing multiple nodes per context switch, but take care to avoid excessive memory usage,
// especially since callers may export several IAVL stores in parallel (e.g. the Cosmos SDK).
const exportBufferSize = 32

// ErrorExportDone is returned by Exporter.Next() when all items have been exported.
var ErrorExportDone = errors.New("export is complete")

// ErrNotInitalizedTree when chains introduce a store without initializing data
var ErrNotInitalizedTree = errors.New("iavl/export newExporter failed to create")

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
	tree       *ImmutableTree
	ch         chan *ExportNode
	cancel     context.CancelFunc
	optimistic bool // export raw key value pairs for optimistic import
}

// NewExporter creates a new Exporter. Callers must call Close() when done.
func newExporter(tree *ImmutableTree) (*Exporter, error) {
	return newExporterWithOptions(tree, false)
}

// NewOptimisticExporter creates a new Exporter with raw Key Values. Callers must call Close() when done.
func newOptimisticExporter(tree *ImmutableTree) (*Exporter, error) {
	return newExporterWithOptions(tree, true)
}

// NewExporterWithOptions creates a new Exporter and configures optimistic mode
func newExporterWithOptions(tree *ImmutableTree, optimistic bool) (*Exporter, error) {
	if tree == nil {
		return nil, fmt.Errorf("tree is nil: %w", ErrNotInitalizedTree)
	}
	// CV Prevent crash on incrVersionReaders if tree.ndb == nil
	if tree.ndb == nil {
		return nil, fmt.Errorf("tree.ndb is nil: %w", ErrNotInitalizedTree)
	}

	ctx, cancel := context.WithCancel(context.Background())
	exporter := &Exporter{
		tree:       tree,
		ch:         make(chan *ExportNode, exportBufferSize),
		cancel:     cancel,
		optimistic: optimistic,
	}

	tree.ndb.incrVersionReaders(tree.version)
	if exporter.optimistic {
		go exporter.optimisticExport(ctx)
	} else {
		go exporter.export(ctx)
	}

	return exporter, nil
}

// export exports nodes
func (e *Exporter) export(ctx context.Context) {
	_ = e.tree.root.traversePost(e.tree, true, func(node *Node) bool {
		exportNode := &ExportNode{
			Key:     node.key,
			Value:   node.value,
			Version: node.nodeKey.version,
			Height:  node.subtreeHeight,
		}

		select {
		case e.ch <- exportNode:
			return false
		case <-ctx.Done():
			return true
		}
	})
	close(e.ch)
}

// optimisticExport exports raw key, value nodes
// Cosmos-SDK should set different snapshot format so nodes can select between either "untrusted statesync" or "trusted-peer optimistic" import
func (e *Exporter) optimisticExport(ctx context.Context) {
	e.tree.root.traverse(e.tree, true, func(node *Node) bool {
		// TODO: How to get the original db value bytes directly without writeBytes()?
		buf := bufPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer bufPool.Put(buf)

		if err := node.writeBytes(buf); err != nil {
			fmt.Printf("WARN: failed writeBytes")
		}

		bytesCopy := make([]byte, buf.Len())
		copy(bytesCopy, buf.Bytes())

		// Use Export Node Format.
		exportNode := &ExportNode{
			Key:     node.GetKey(), // TODO: How to get prefixed key so that import does not need to prefix?
			Value:   bytesCopy,
			Version: 0, // Version not used
			Height:  0, // Height not used
		}

		select {
		case e.ch <- exportNode:
			return false
		case <-ctx.Done():
			return true
		}
	})
	close(e.ch)
}

// Next fetches the next exported node, or returns ExportDone when done.
func (e *Exporter) Next() (*ExportNode, error) {
	if exportNode, ok := <-e.ch; ok {
		return exportNode, nil
	}
	return nil, ErrorExportDone
}

// Close closes the exporter. It is safe to call multiple times.
func (e *Exporter) Close() {
	e.cancel()
	for range e.ch { //nolint:revive
	} // drain channel
	if e.tree != nil {
		e.tree.ndb.decrVersionReaders(e.tree.version)
	}
	e.tree = nil
}
