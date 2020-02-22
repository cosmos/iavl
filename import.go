package iavl

import (
	"bytes"
	"fmt"

	"github.com/pkg/errors"
	db "github.com/tendermint/tm-db"
)

var (
	ErrNoImport = errors.New("no import in progress")
)

// Importer imports data into an empty database
type Importer struct {
	tree    *MutableTree
	version int64
	batch   db.Batch
	stack   []*Node
}

// NewImporter creates a new Importer. Callers must call Done() when done.
func NewImporter(tree *MutableTree, version int64) (*Importer, error) {
	if version <= 0 {
		return nil, errors.New("imported version must be greater than 0")
	}
	tree.ndb.mtx.Lock()
	if tree.ndb.latestVersion > 0 {
		tree.ndb.mtx.Unlock()
		return nil, errors.Errorf("found database at version %d, must be 0", tree.ndb.latestVersion)
	}

	return &Importer{
		tree:    tree,
		version: version,
		batch:   tree.ndb.snapshotDB.NewBatch(),
		stack:   make([]*Node, 0, 8),
	}, nil
}

// close closes all resources, i.e. releases the mutex and frees the batch.
func (i *Importer) close() {
	if i.tree == nil {
		return
	}
	i.batch.Close()
	i.tree.ndb.mtx.Unlock()
	i.tree = nil
}

// error is a convenience function which cancels the import and returns an error.
func (i *Importer) error(msg interface{}, args ...interface{}) error {
	i.close()
	switch e := msg.(type) {
	case error:
		return e
	case string:
		return errors.Errorf(e, args)
	case fmt.Stringer:
		return errors.New(e.String())
	default:
		return errors.New("unknown error")
	}
}

// Cancel cancels the import, rolling back writes and releasing the mutex.
func (i *Importer) Cancel() {
	i.close()
}

// Import imports an item into the database.
func (i *Importer) Import(item ExportNode) error {
	if i.tree == nil {
		return ErrNoImport
	}
	return i.importMinimal(item)
	//return i.importNDB(item)
}

// importMinimal imports minimal nodes
func (i *Importer) importMinimal(item ExportNode) error {
	if item.Version > i.version {
		return i.error("Node version %v can't be greater than import version %v",
			item.Version, i.version)
	}

	node := &Node{
		key:     item.Key,
		value:   item.Value,
		version: item.Version,
		height:  item.Height,
	}

	stackSize := len(i.stack)
	switch {
	case stackSize >= 2 && i.stack[stackSize-1].height < node.height && i.stack[stackSize-2].height < node.height:
		node.leftNode = i.stack[stackSize-2]
		node.leftHash = node.leftNode.hash
		node.rightNode = i.stack[stackSize-1]
		node.rightHash = node.rightNode.hash
		i.stack = i.stack[:stackSize-2]
	case stackSize >= 1 && i.stack[stackSize-1].height < node.height:
		node.leftNode = i.stack[stackSize-1]
		node.leftHash = node.leftNode.hash
		i.stack = i.stack[:stackSize-1]
	}

	if node.height == 0 {
		node.size = 1
	}
	if node.leftNode != nil {
		node.size += node.leftNode.size
	}
	if node.rightNode != nil {
		node.size += node.rightNode.size
	}

	node._hash()
	i.stack = append(i.stack, node)

	var buf bytes.Buffer
	err := node.writeBytes(&buf)
	if err != nil {
		return i.error(err)
	}

	i.batch.Set(i.tree.ndb.nodeKey(node.hash), buf.Bytes())

	return nil
}

// importNDB imports nodes from NDB byte slices
/*func (i *Importer) importNDB(item ExportNode) error {
	node, err := MakeNode(item)
	if err != nil {
		return i.error(err)
	}
	node._hash()

	if node.version > i.version {
		return i.error("Node version %v can't be greater than import version %v",
			node.version, i.version)
	}

	i.batch.Set(i.tree.ndb.nodeKey(node.hash), item)

	if !i.rootSeen {
		i.batch.Set(i.tree.ndb.rootKey(i.version), node.hash)
		i.rootSeen = true
	}

	return nil
}*/

// Done finishes the import.
func (i *Importer) Done() error {
	if i.tree == nil {
		return ErrNoImport
	}

	switch {
	case len(i.stack) == 1:
		i.batch.Set(i.tree.ndb.rootKey(i.version), i.stack[0].hash)
	case len(i.stack) > 2:
		return errors.Errorf("invalid node structure, found stack size %v when finalizing", len(i.stack))
	}

	err := i.batch.WriteSync()
	if err != nil {
		return i.error(err)
	}
	i.tree.ndb.updateLatestVersion(i.version)

	root, err := i.tree.ndb.getRoot(i.version)
	if err != nil {
		return i.error(err)
	}
	if len(root) > 0 {
		i.tree.ImmutableTree.root = i.tree.ndb.getNode(root)
	}

	i.tree.versions[i.version] = true
	i.tree.version = i.version

	if len(root) > 0 {
		last, err := i.tree.GetImmutable(i.version)
		if err != nil {
			return i.error(err)
		}
		i.tree.lastSaved = last
	}

	i.close()
	return nil
}
