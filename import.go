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
	tree     *MutableTree
	version  int64
	batch    db.Batch
	rootSeen bool
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

	node, err := item.ToNode()
	if err != nil {
		return i.error(err)
	}

	if node.version > i.version {
		return i.error("Node version %v can't be greater than import version %v",
			node.version, i.version)
	}

	if ens, ok := item.(*ExportNodeNDB); ok {
		i.batch.Set(i.tree.ndb.nodeKey(node.hash), ens.bytes)
	} else {
		var buf bytes.Buffer
		err := node.writeBytes(&buf)
		if err != nil {
			panic(err)
		}
		i.batch.Set(i.tree.ndb.nodeKey(node.hash), buf.Bytes())
	}

	if !i.rootSeen {
		i.batch.Set(i.tree.ndb.rootKey(i.version), node.hash)
		i.rootSeen = true
	}

	return nil
}

// Done finishes the import.
func (i *Importer) Done() error {
	if i.tree == nil {
		return ErrNoImport
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
