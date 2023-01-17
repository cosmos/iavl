package iavl

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"

	db "github.com/tendermint/tm-db"
)

// maxBatchSize is the maximum size of the import batch before flushing it to the database
const maxBatchSize = 20000

// ErrNoImport is returned when calling methods on a closed importer
var ErrNoImport = errors.New("no import in progress")
var TotalAggregateBytes int64
var TotalSerializeLatency int64
var TotalSetLatency int64
var TotalBatchWriteLatency int64

// Importer imports data into an empty MutableTree. It is created by MutableTree.Import(). Users
// must call Close() when done.
//
// ExportNodes must be imported in the order returned by Exporter, i.e. depth-first post-order (LRN).
//
// Importer is not concurrency-safe, it is the caller's responsibility to ensure the tree is not
// modified while performing an import.
type Importer struct {
	tree         *MutableTree
	version      int64
	batch        db.Batch
	batchMutex   sync.RWMutex
	batchSize    uint32
	stack        []*Node
	chBatch      chan db.Batch
	chBatchWg    sync.WaitGroup
	chNode       chan Node
	chNodeWg     sync.WaitGroup
	chNodeData   chan NodeData
	chNodeDataWg sync.WaitGroup
}

type NodeData struct {
	node Node
	data []byte
}

// newImporter creates a new Importer for an empty MutableTree.
//
// version should correspond to the version that was initially exported. It must be greater than
// or equal to the highest ExportNode version number given.
func newImporter(tree *MutableTree, version int64) (*Importer, error) {
	if version < 0 {
		return nil, errors.New("imported version cannot be negative")
	}
	if tree.ndb.latestVersion > 0 {
		return nil, errors.Errorf("found database at version %d, must be 0", tree.ndb.latestVersion)
	}
	if !tree.IsEmpty() {
		return nil, errors.New("tree must be empty")
	}

	var importer = &Importer{
		tree:         tree,
		version:      version,
		batch:        tree.ndb.db.NewBatch(),
		batchMutex:   sync.RWMutex{},
		stack:        make([]*Node, 0, 8),
		chBatch:      make(chan db.Batch, 1),
		chBatchWg:    sync.WaitGroup{},
		chNode:       make(chan Node, maxBatchSize),
		chNodeWg:     sync.WaitGroup{},
		chNodeData:   make(chan NodeData, maxBatchSize),
		chNodeDataWg: sync.WaitGroup{},
	}

	importer.chBatchWg.Add(1)
	go periodicBatchCommit(importer)

	importer.chNodeWg.Add(1)
	go serializeAsync(importer)

	importer.chNodeDataWg.Add(1)
	go writeNodeData(importer)

	return importer, nil
}

func periodicBatchCommit(i *Importer) {
	for i.batch != nil {
		nextBatch, chanOpen := <-i.chBatch
		if !chanOpen {
			break
		}
		batchWriteStart := time.Now().UnixMicro()
		err := nextBatch.Write()
		if err != nil {
			panic(err)
		}
		fmt.Println("Closing batch after batch write done")
		nextBatch.Close()
		batchWriteEnd := time.Now().UnixMicro()
		batchCommitLatency := batchWriteEnd - batchWriteStart
		TotalBatchWriteLatency += batchCommitLatency
		fmt.Printf("[IAVL IMPORTER] Batch commit latency: %d\n", batchCommitLatency/1000)

	}
	i.chBatchWg.Done()
	fmt.Printf("[IAVL IMPORTER] Shutting down the batch commit thread\n")
}

func serializeAsync(i *Importer) {
	for i.batch != nil {
		start := time.Now().UnixMicro()
		currNode, chanOpen := <-i.chNode

		if !chanOpen {
			break
		}

		err := currNode.validate()
		if err != nil {
			panic(err)
		}

		buf := bufPool.Get().(*bytes.Buffer)
		buf.Reset()
		if err = currNode.writeBytes(buf); err != nil {
			panic(err)
		}
		bytesCopy := make([]byte, buf.Len())
		copy(bytesCopy, buf.Bytes())

		bufPool.Put(buf)

		TotalAggregateBytes += int64(len(bytesCopy))
		TotalSerializeLatency += time.Now().UnixMicro() - start
		i.chNodeData <- NodeData{
			node: currNode,
			data: bytesCopy,
		}

	}
	i.chNodeWg.Done()
	fmt.Printf("[IAVL IMPORTER] Shutting down the serializeAsync thread\n")
}

func writeNodeData(i *Importer) {
	for i.batch != nil {
		nodeData, chanOpen := <-i.chNodeData
		if !chanOpen {
			break
		}
		start := time.Now().UnixMicro()
		i.batchMutex.RLock()
		if i.batch != nil {
			err := i.batch.Set(i.tree.ndb.nodeKey(nodeData.node.hash), nodeData.data)
			if err != nil {
				panic(err)
			}
		}
		TotalSetLatency += time.Now().UnixMicro() - start
		i.batchMutex.RUnlock()
		i.batchSize++
		if i.batchSize >= maxBatchSize && len(i.chBatch) < 1 {
			i.chBatch <- i.batch
			i.batch = i.tree.ndb.db.NewBatch()
			i.batchSize = 0
		}
	}
	i.chNodeDataWg.Done()
	fmt.Printf("[IAVL IMPORTER] Shutting down the writeNodeData thread\n")
}

// Close frees all resources. It is safe to call multiple times. Uncommitted nodes may already have
// been flushed to the database, but will not be visible.
func (i *Importer) Close() {
	i.batchMutex.Lock()
	defer i.batchMutex.Unlock()
	fmt.Println("Acquired lock in close")

	if i.batch != nil {
		fmt.Println("Closing batch now!!!")
		i.batch.Close()
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
		return errors.Errorf("node version %v can't be greater than import version %v",
			exportNode.Version, i.version)
	}

	node := &Node{
		key:           exportNode.Key,
		value:         exportNode.Value,
		version:       exportNode.Version,
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
	switch {
	case stackSize >= 2 && i.stack[stackSize-1].subtreeHeight < node.subtreeHeight && i.stack[stackSize-2].subtreeHeight < node.subtreeHeight:
		node.leftNode = i.stack[stackSize-2]
		node.leftHash = node.leftNode.hash
		node.rightNode = i.stack[stackSize-1]
		node.rightHash = node.rightNode.hash
	case stackSize >= 1 && i.stack[stackSize-1].subtreeHeight < node.subtreeHeight:
		node.leftNode = i.stack[stackSize-1]
		node.leftHash = node.leftNode.hash
	}

	if node.subtreeHeight == 0 {
		node.size = 1
	}
	if node.leftNode != nil {
		node.size += node.leftNode.size
	}
	if node.rightNode != nil {
		node.size += node.rightNode.size
	}
	node._hash()

	i.chNode <- *node

	// Update the stack now that we know there were no errors
	switch {
	case node.leftHash != nil && node.rightHash != nil:
		i.stack = i.stack[:stackSize-2]
	case node.leftHash != nil || node.rightHash != nil:
		i.stack = i.stack[:stackSize-1]
	}

	// Only hash\height\size of the node will be used after it be pushed into the stack.
	i.stack = append(i.stack, &Node{hash: node.hash, subtreeHeight: node.subtreeHeight, size: node.size})

	return nil
}

// Commit finalizes the import by flushing any outstanding nodes to the database, making the
// version visible, and updating the tree metadata. It can only be called once, and calls Close()
// internally.
func (i *Importer) Commit() error {
	fmt.Println("[IAVL] Waiting to start commit")
	for len(i.chNodeData) > 0 || len(i.chNode) > 0 || len(i.chBatch) > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	close(i.chNode)
	i.chNodeWg.Wait()
	close(i.chNodeData)
	i.chNodeDataWg.Wait()
	close(i.chBatch)
	i.chBatchWg.Wait()

	fmt.Println("[IAVL] Starting to commit")

	if i.tree == nil {
		return ErrNoImport
	}

	switch len(i.stack) {
	case 0:
		fmt.Println("[IAVL] Writing something for case 0")
		if err := i.batch.Set(i.tree.ndb.rootKey(i.version), []byte{}); err != nil {
			panic(err)
		}
	case 1:
		fmt.Println("[IAVL] Writing something for case 1")
		if err := i.batch.Set(i.tree.ndb.rootKey(i.version), i.stack[0].hash); err != nil {
			panic(err)
		}
	default:
		return errors.Errorf("invalid node structure, found stack size %v when committing",
			len(i.stack))
	}

	fmt.Println("[IAVL] Committing batch with write sync")
	err := i.batch.WriteSync()
	if err != nil {
		fmt.Printf("[IAVL] Committing batch hitting some err: %s", err.Error())
		return err
	}
	fmt.Println("[IAVL] Resetting latest version ")
	i.tree.ndb.resetLatestVersion(i.version)
	fmt.Println("[IAVL] Loading version ")
	_, err = i.tree.LoadVersion(i.version)
	fmt.Println("[IAVL] Loaded version ")
	if err != nil {
		return err
	}

	fmt.Printf("[IAVL] Closing batch after commit(), total size is %d \n", TotalAggregateBytes)
	fmt.Printf("[IAVL] TotalSerialize latency is: %d, TotalSetDataLatency is: %d, TotalBatchWriteLatency is:%d \n", TotalSerializeLatency, TotalSetLatency, TotalBatchWriteLatency)

	i.Close()
	return nil
}
