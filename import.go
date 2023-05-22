package iavl

import (
	"bytes"
	"errors"
	"fmt"
	db "github.com/cosmos/cosmos-db"
	"sync"
)

// desiredBatchSize is the desired batch write size of the import batch before flushing it to the database.
// The actual batch write size could exceed this value when the previous batch is still flushing.
const defaultDesiredBatchSize = 20000

// If there's an ongoing pending batch write, we will keep batching more writes
// until the ongoing batch write completes or we reach maxBatchSize
const defaultMaxBatchSize = 500000

// ErrNoImport is returned when calling methods on a closed importer
var ErrNoImport = errors.New("no import in progress")

// Importer imports data into an empty MutableTree. It is created by MutableTree.Import(). Users
// must call Close() when done.
//
// ExportNodes must be imported in the order returned by Exporter, i.e. depth-first post-order (LRN).
//
// Importer is not concurrency-safe, it is the caller's responsibility to ensure the tree is not
// modified while performing an import.
type Importer struct {
	tree       *MutableTree
	version    int64
	batch      db.Batch
	batchSize  uint32
	batchMtx   sync.RWMutex
	stack      []*Node
	nonces     []uint32
	chanConfig ChannelConfig
}

type ChannelConfig struct {
	desiredBatchSize uint32
	maxBatchSize     uint32
	chNodeData       chan NodeData
	chNodeDataWg     sync.WaitGroup
	chBatch          chan db.Batch
	chBatchWg        sync.WaitGroup
	chError          chan error
	allChannelClosed bool
}

func DefaultChannelConfig() ChannelConfig {
	return ChannelConfig{
		desiredBatchSize: defaultDesiredBatchSize,
		maxBatchSize:     defaultMaxBatchSize,
		chNodeData:       make(chan NodeData, 2*defaultDesiredBatchSize),
		chNodeDataWg:     sync.WaitGroup{},
		chBatch:          make(chan db.Batch, 1),
		chBatchWg:        sync.WaitGroup{},
		chError:          make(chan error, 1),
		allChannelClosed: false,
	}
}

type NodeData struct {
	node *Node
	data []byte
}

// newImporter creates a new Importer for an empty MutableTree.
// Underneath it spawns three goroutines to process the data import flow.
//
// version should correspond to the version that was initially exported. It must be greater than
// or equal to the highest ExportNode version number given.
func newImporter(tree *MutableTree, version int64) (*Importer, error) {
	if version < 0 {
		return nil, errors.New("imported version cannot be negative")
	}
	if tree.ndb.latestVersion > 0 {
		return nil, fmt.Errorf("found database at version %d, must be 0", tree.ndb.latestVersion)
	}
	if !tree.IsEmpty() {
		return nil, errors.New("tree must be empty")
	}

	importer := &Importer{
		tree:       tree,
		version:    version,
		batch:      tree.ndb.db.NewBatch(),
		stack:      make([]*Node, 0, 8),
		nonces:     make([]uint32, version+1),
		batchMtx:   sync.RWMutex{},
		chanConfig: DefaultChannelConfig(),
	}

	importer.chanConfig.chNodeDataWg.Add(1)
	go setBatchData(importer)

	importer.chanConfig.chBatchWg.Add(1)
	go batchWrite(importer)

	return importer, nil
}

// setBatchData get the next serialized node data from channel, and write the data to the current batch
func setBatchData(i *Importer) {
	for i.batch != nil {
		if nodeData, open := <-i.chanConfig.chNodeData; open {
			i.batchMtx.RLock()
			if i.batch != nil {
				err := i.batch.Set(i.tree.ndb.nodeKey(nodeData.node.GetKey()), nodeData.data)
				if err != nil {
					i.batchMtx.RUnlock()
					i.chanConfig.chError <- err
					break
				}
			}
			i.batchSize++
			i.batchMtx.RUnlock()
			// Only commit a new batch if size meet desiredBatchSize and there's no pending batch write or we exceed maxBatchSize
			if (i.batchSize >= i.chanConfig.desiredBatchSize && len(i.chanConfig.chBatch) < 1) || i.batchSize >= i.chanConfig.maxBatchSize {
				i.chanConfig.chBatch <- i.batch
				i.batch = i.tree.ndb.db.NewBatch()
				i.batchSize = 0
			}
		} else {
			break
		}
	}
	i.chanConfig.chNodeDataWg.Done()
}

// batchWrite get a new batch from the channel and execute the batch write to the underlying DB.
func batchWrite(i *Importer) {
	for i.batch != nil {
		if nextBatch, open := <-i.chanConfig.chBatch; open {
			err := nextBatch.Write()
			if err != nil {
				i.chanConfig.chError <- err
				break
			}
			i.batchMtx.Lock()
			nextBatch.Close()
			i.batchMtx.Unlock()
		} else {
			break
		}
	}
	i.chanConfig.chBatchWg.Done()
}

// writeNode writes the node content to the storage.
func (i *Importer) writeNode(node *Node) error {
	if _, err := node._hash(node.nodeKey.version); err != nil {
		return err
	}
	if err := node.validate(); err != nil {
		return err
	}

	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	if err := node.writeBytes(buf); err != nil {
		return err
	}

	bytesCopy := make([]byte, buf.Len())
	copy(bytesCopy, buf.Bytes())
	bufPool.Put(buf)

	// Handle the remaining steps in a separate goroutine
	i.chanConfig.chNodeData <- NodeData{
		node: node,
		data: bytesCopy,
	}

	return nil
}

// Close frees all resources. It is safe to call multiple times. Uncommitted nodes may already have
// been flushed to the database, but will not be visible.
func (i *Importer) Close() {
	_ = i.waitAndCloseChannels()
	if i.batch != nil {
		_ = i.batch.Close()
	}
	i.batch = nil
	i.tree = nil
}

// Add adds an ExportNode to the import. ExportNodes must be added in the order returned by
// Exporter, i.e. depth-first post-order (LRN). Nodes are periodically flushed to the database,
// but the imported version is not visible until Commit() is called.
func (i *Importer) Add(exportNode *ExportNode) error {
	if i.tree == nil {
		return ErrNoImport
	}
	if exportNode == nil {
		return errors.New("node cannot be nil")
	}
	if exportNode.Version > i.version {
		return fmt.Errorf("node version %v can't be greater than import version %v",
			exportNode.Version, i.version)
	}

	node := &Node{
		key:           exportNode.Key,
		value:         exportNode.Value,
		subtreeHeight: exportNode.Height,
	}

	// We build the tree from the bottom-left up. The stack is used to store unresolved left
	// children while constructing right children. When all children are built, the parent can
	// be constructed and the resolved children can be discarded from the stack. Using a stack
	// ensures that we can handle additional unresolved left children while building a right branch.
	//
	// We don't modify the stack until we've verified the built node, to avoid leaving the
	// importer in an inconsistent state when we return an error.
	stackSize := len(i.stack)
	if node.subtreeHeight == 0 {
		node.size = 1
	} else if stackSize >= 2 && i.stack[stackSize-1].subtreeHeight < node.subtreeHeight && i.stack[stackSize-2].subtreeHeight < node.subtreeHeight {
		leftNode := i.stack[stackSize-2]
		rightNode := i.stack[stackSize-1]

		node.leftNode = leftNode
		node.rightNode = rightNode
		node.leftNodeKey = leftNode.GetKey()
		node.rightNodeKey = rightNode.GetKey()
		node.size = leftNode.size + rightNode.size

		// Update the stack now.
		if err := i.writeNode(leftNode); err != nil {
			return err
		}
		if err := i.writeNode(rightNode); err != nil {
			return err
		}

		// Check errors from channel
		select {
		case err := <-i.chanConfig.chError:
			return err
		default:
		}

		i.stack = i.stack[:stackSize-2]

		// remove the recursive references to avoid memory leak
		leftNode.leftNode = nil
		leftNode.rightNode = nil
		rightNode.leftNode = nil
		rightNode.rightNode = nil
	}
	i.nonces[exportNode.Version]++
	node.nodeKey = &NodeKey{
		version: exportNode.Version,
		// Nonce is 1-indexed, but start at 2 since the root node having a nonce of 1.
		nonce: i.nonces[exportNode.Version] + 1,
	}

	i.stack = append(i.stack, node)

	return nil
}

// Commit finalizes the import by flushing any outstanding nodes to the database, making the
// version visible, and updating the tree metadata. It can only be called once, and calls Close()
// internally.
func (i *Importer) Commit() error {
	if i.tree == nil {
		return ErrNoImport
	}

	err := i.waitAndCloseChannels()
	if err != nil {
		return err
	}

	switch len(i.stack) {
	case 0:
		if err := i.batch.Set(i.tree.ndb.nodeKey(GetRootKey(i.version)), []byte{}); err != nil {
			return err
		}
	case 1:
		i.stack[0].nodeKey.nonce = 1
		if err := i.writeNode(i.stack[0]); err != nil {
			return err
		}
		if i.stack[0].nodeKey.version < i.version { // it means there is no update in the given version
			if err := i.batch.Set(i.tree.ndb.nodeKey(GetRootKey(i.version)), i.tree.ndb.nodeKey(i.stack[0].GetKey())); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("invalid node structure, found stack size %v when committing",
			len(i.stack))
	}

	err = i.batch.WriteSync()
	if err != nil {
		return err
	}
	i.tree.ndb.resetLatestVersion(i.version)

	_, err = i.tree.LoadVersion(i.version)
	if err != nil {
		return err
	}

	i.Close()
	return nil
}

// waitAndCloseChannels will try to close all the channels for importer and wait for remaining work to be done.
// This function should only be called in the Commit or Close action. If any error happens when draining the remaining data in the channel,
// The error will be popped out and returned.
func (i *Importer) waitAndCloseChannels() error {
	// Make sure all pending works are drained and close the channels in order
	if !i.chanConfig.allChannelClosed {
		i.chanConfig.allChannelClosed = true
		close(i.chanConfig.chNodeData)
		i.chanConfig.chNodeDataWg.Wait()
		close(i.chanConfig.chBatch)
		i.chanConfig.chBatchWg.Wait()
		// Check errors
		select {
		case err := <-i.chanConfig.chError:
			return err
		default:
		}
	}
	return nil
}
