package iavl

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"github.com/tendermint/tendermint/crypto/tmhash"
	dbm "github.com/tendermint/tendermint/libs/db"
)

const (
	int64Size = 8
	hashSize  = tmhash.Size
)

var (
	// All node keys are prefixed with the byte 'n'. This ensures no collision is
	// possible with the other keys, and makes them easier to traverse. They are indexed by the node hash.
	nodeKeyFormat = NewKeyFormat('n', hashSize) // n<hash>

	// Orphans are keyed in the database by their expected lifetime.
	// The first number represents the *last* version at which the orphan needs
	// to exist, while the second number represents the *earliest* version at
	// which it is expected to exist - which starts out by being the version
	// of the node being orphaned.
	orphanKeyFormat = NewKeyFormat('o', int64Size, int64Size, hashSize) // o<last-version><first-version><hash>

	// Root nodes are indexed separately by their version
	rootKeyFormat = NewKeyFormat('r', int64Size) // r<version>
)

type nodeDB struct {
	mtx   sync.Mutex // Read/write lock.
	db    dbm.DB     // Persistent node storage.
	dbMem dbm.DB     // Memory node storage.
	batch dbm.Batch  // Batched writing buffer.

	latestVersion int64
}

func newNodeDB(db dbm.DB, cacheSize int) *nodeDB {
	ndb := &nodeDB{
		db:            db,
		dbMem:         dbm.NewMemDB(),
		batch:         db.NewBatch(),
		latestVersion: 0, // initially invalid
	}
	return ndb
}

// GetNode gets a node from memory or disk. If it is an inner node, it does not
// load its children.
func (ndb *nodeDB) GetNode(hash []byte) *Node {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	if len(hash) == 0 {
		panic("nodeDB.GetNode() requires hash")
	}

	//Try reading from memory
	buf := ndb.dbMem.Get(ndb.nodeKey(hash))
	if buf == nil {
		// Doesn't exist, load from disk
		buf = ndb.db.Get(ndb.nodeKey(hash))
		if buf == nil {
			panic(fmt.Sprintf("Value missing for hash %x corresponding to nodeKey %s", hash, ndb.nodeKey(hash)))
		}
	}

	node, err := MakeNode(buf)
	if err != nil {
		panic(fmt.Sprintf("Error reading Node. bytes: %x, error: %v", buf, err))
	}

	node.hash = hash
	node.persisted = true

	return node
}

// SaveNode saves a node to disk.
func (ndb *nodeDB) SaveNode(node *Node, flushToDisk bool) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	if node.hash == nil {
		panic("Expected to find node.hash, but none found.")
	}
	if node.persisted {
		panic("Shouldn't be calling save on an already persisted node.")
	}

	// Save node bytes to db.
	buf := new(bytes.Buffer)
	if err := node.writeBytes(buf); err != nil {
		panic(err)
	}
	if flushToDisk == false {
		ndb.batch.Set(ndb.nodeKey(node.hash), buf.Bytes())
	} else {
		ndb.batch.Set(ndb.nodeKey(node.hash), buf.Bytes())
	}
	debug("BATCH SAVE %X %p\n", node.hash, node)

	if flushToDisk == true {
		node.persisted = true
	}
}

// Has checks if a hash exists in the database.
func (ndb *nodeDB) Has(hash []byte) bool {
	key := ndb.nodeKey(hash)

	if ldb, ok := ndb.db.(*dbm.GoLevelDB); ok {
		exists, err := ldb.DB().Has(key, nil)
		if err != nil {
			panic("Got error from leveldb: " + err.Error())
		}
		return exists
	}
	return ndb.db.Get(key) != nil
}

// SaveBranch saves the given node and all of its descendants.
// NOTE: This function clears leftNode/rigthNode recursively and
// calls _hash() on the given node.
// TODO refactor, maybe use hashWithCount() but provide a callback.
func (ndb *nodeDB) SaveBranch(node *Node, flushToDisk bool) []byte {
	if node.persisted {
		return node.hash
	}

	if node.leftNode != nil {
		node.leftHash = ndb.SaveBranch(node.leftNode, flushToDisk)
	}
	if node.rightNode != nil {
		node.rightHash = ndb.SaveBranch(node.rightNode, flushToDisk)
	}

	node._hash()
	ndb.SaveNode(node, flushToDisk)

	node.leftNode = nil
	node.rightNode = nil

	return node.hash
}

// DeleteVersion deletes a tree version from disk.
func (ndb *nodeDB) DeleteVersion(version int64, checkLatestVersion bool) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	ndb.deleteOrphans(version)
	ndb.deleteRoot(version, checkLatestVersion)
}

// Saves orphaned nodes to disk under a special prefix.
// version: the new version being saved.
// orphans: the orphan nodes created since version-1
func (ndb *nodeDB) SaveOrphans(version int64, orphans map[string]int64, flushToDisk bool) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	var toVersion int64
	if flushToDisk == true {
		toVersion = ndb.getPreviousVersion(version)
	} else {
		//UGH
		toVersion = ndb.getPreviousVersion2(version)
	}
	for hash, fromVersion := range orphans {
		if fromVersion > toVersion && flushToDisk == false {
			//toVersion2 := ndb.getPreviousVersion(version)

			//fmt.Printf("In theory we dont need to save an orphan that was deleted in a version in memory %v-%v %X (old db-%v)\n", fromVersion, toVersion, hash, toVersion2)
			continue
		}

		debug("SAVEORPHAN %v-%v %X\n", fromVersion, toVersion, hash)
		ndb.saveOrphan([]byte(hash), fromVersion, toVersion)
	}
}

// Saves a single orphan to disk.
func (ndb *nodeDB) saveOrphan(hash []byte, fromVersion, toVersion int64) {
	if fromVersion > toVersion {
		panic(fmt.Sprintf("Orphan expires before it comes alive.  %d > %d", fromVersion, toVersion))
	}
	key := ndb.orphanKey(fromVersion, toVersion, hash)
	ndb.batch.Set(key, hash)
}

// deleteOrphans deletes orphaned nodes from disk, and the associated orphan
// entries.
func (ndb *nodeDB) deleteOrphans(version int64) {
	// Will be zero if there is no previous version.
	predecessor := ndb.getPreviousVersion(version)

	// Traverse orphans with a lifetime ending at the version specified.
	// TODO optimize.
	ndb.traverseOrphansVersion(version, func(key, hash []byte) {
		var fromVersion, toVersion int64

		// See comment on `orphanKeyFmt`. Note that here, `version` and
		// `toVersion` are always equal.
		orphanKeyFormat.Scan(key, &toVersion, &fromVersion)

		// Delete orphan key and reverse-lookup key.
		ndb.batch.Delete(key)

		// If there is no predecessor, or the predecessor is earlier than the
		// beginning of the lifetime (ie: negative lifetime), or the lifetime
		// spans a single version and that version is the one being deleted, we
		// can delete the orphan.  Otherwise, we shorten its lifetime, by
		// moving its endpoint to the previous version.
		if predecessor < fromVersion || fromVersion == toVersion {
			debug("DELETE predecessor:%v fromVersion:%v toVersion:%v %X\n", predecessor, fromVersion, toVersion, hash)
			ndb.batch.Delete(ndb.nodeKey(hash))
		} else {
			debug("MOVE predecessor:%v fromVersion:%v toVersion:%v %X\n", predecessor, fromVersion, toVersion, hash)
			ndb.saveOrphan(hash, fromVersion, predecessor)
		}
	})
}

func (ndb *nodeDB) nodeKey(hash []byte) []byte {
	return nodeKeyFormat.KeyBytes(hash)
}

func (ndb *nodeDB) orphanKey(fromVersion, toVersion int64, hash []byte) []byte {
	return orphanKeyFormat.Key(toVersion, fromVersion, hash)
}

func (ndb *nodeDB) rootKey(version int64) []byte {
	return rootKeyFormat.Key(version)
}

func (ndb *nodeDB) getLatestVersion() int64 {
	if ndb.latestVersion == 0 {
		ndb.latestVersion = ndb.getPreviousVersion(1<<63 - 1)
	}
	return ndb.latestVersion
}

func (ndb *nodeDB) updateLatestVersion(version int64) {
	if ndb.latestVersion < version {
		ndb.latestVersion = version
	}
}

func (ndb *nodeDB) resetLatestVersion(version int64) {
	ndb.latestVersion = version
}

func (ndb *nodeDB) getPreviousVersion(version int64) int64 {
	return ndb.getPreviousVersioni(version, false)
}

func (ndb *nodeDB) getPreviousVersion2(version int64) int64 {
	return ndb.getPreviousVersioni(version, true)
}

func (ndb *nodeDB) getPreviousVersioni(version int64, mem bool) int64 {
	itr := ndb.db.ReverseIterator(
		rootKeyFormat.Key(1),
		rootKeyFormat.Key(version),
	)
	defer itr.Close()

	pversion := int64(-1)
	for ; itr.Valid(); itr.Next() {
		k := itr.Key()
		rootKeyFormat.Scan(k, &pversion)
		return pversion
	}

	return 0
}

// deleteRoot deletes the root entry from disk, but not the node it points to.
func (ndb *nodeDB) deleteRoot(version int64, checkLatestVersion bool) {
	if checkLatestVersion && version == ndb.getLatestVersion() {
		panic("Tried to delete latest version")
	}

	key := ndb.rootKey(version)
	ndb.batch.Delete(key)
}

func (ndb *nodeDB) traverseOrphans(fn func(k, v []byte)) {
	ndb.traversePrefix(orphanKeyFormat.Key(), fn)
}

// Traverse orphans ending at a certain version.
func (ndb *nodeDB) traverseOrphansVersion(version int64, fn func(k, v []byte)) {
	ndb.traversePrefix(orphanKeyFormat.Key(version), fn)
}

// Traverse all keys.
func (ndb *nodeDB) traverse(fn func(key, value []byte)) {
	itr := ndb.db.Iterator(nil, nil)
	defer itr.Close()

	for ; itr.Valid(); itr.Next() {
		fn(itr.Key(), itr.Value())
	}
}

// Traverse all keys with a certain prefix.
func (ndb *nodeDB) traversePrefix(prefix []byte, fn func(k, v []byte)) {
	itr := dbm.IteratePrefix(ndb.db, prefix)
	defer itr.Close()

	for ; itr.Valid(); itr.Next() {
		fn(itr.Key(), itr.Value())
	}
}

// Write to disk.
func (ndb *nodeDB) Commit() {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	ndb.batch.Write()
	ndb.batch.Close()

}

func (ndb *nodeDB) getRoot(version int64) []byte {
	return ndb.db.Get(ndb.rootKey(version))
}

func (ndb *nodeDB) getRoots() (map[int64][]byte, error) {
	roots := map[int64][]byte{}

	ndb.traversePrefix(rootKeyFormat.Key(), func(k, v []byte) {
		var version int64
		rootKeyFormat.Scan(k, &version)
		roots[version] = v
	})
	return roots, nil
}

// SaveRoot creates an entry on disk for the given root, so that it can be
// loaded later.
func (ndb *nodeDB) SaveRoot(root *Node, version int64) error {
	if len(root.hash) == 0 {
		panic("Hash should not be empty")
	}
	return ndb.saveRoot(root.hash, version)
}

// SaveEmptyRoot creates an entry on disk for an empty root.
func (ndb *nodeDB) SaveEmptyRoot(version int64) error {
	return ndb.saveRoot([]byte{}, version)
}

func (ndb *nodeDB) saveRoot(hash []byte, version int64) error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	if version != ndb.getLatestVersion()+1 {
		return fmt.Errorf("Must save consecutive versions. Expected %d, got %d", ndb.getLatestVersion()+1, version)
	}

	key := ndb.rootKey(version)
	ndb.batch.Set(key, hash)
	ndb.updateLatestVersion(version)

	return nil
}

////////////////// Utility and test functions /////////////////////////////////

func (ndb *nodeDB) leafNodes() []*Node {
	leaves := []*Node{}

	ndb.traverseNodes(func(hash []byte, node *Node) {
		if node.isLeaf() {
			leaves = append(leaves, node)
		}
	})
	return leaves
}

func (ndb *nodeDB) nodes() []*Node {
	nodes := []*Node{}

	ndb.traverseNodes(func(hash []byte, node *Node) {
		nodes = append(nodes, node)
	})
	return nodes
}

func (ndb *nodeDB) orphans() [][]byte {
	orphans := [][]byte{}

	ndb.traverseOrphans(func(k, v []byte) {
		orphans = append(orphans, v)
	})
	return orphans
}

func (ndb *nodeDB) roots() map[int64][]byte {
	roots, _ := ndb.getRoots()
	return roots
}

// Not efficient.
// NOTE: DB cannot implement Size() because
// mutations are not always synchronous.
func (ndb *nodeDB) size() int {
	size := 0
	ndb.traverse(func(k, v []byte) {
		size++
	})
	return size
}

func (ndb *nodeDB) traverseNodes(fn func(hash []byte, node *Node)) {
	nodes := []*Node{}

	ndb.traversePrefix(nodeKeyFormat.Key(), func(key, value []byte) {
		node, err := MakeNode(value)
		if err != nil {
			panic(fmt.Sprintf("Couldn't decode node from database: %v", err))
		}
		nodeKeyFormat.Scan(key, &node.hash)
		nodes = append(nodes, node)
	})

	sort.Slice(nodes, func(i, j int) bool {
		return bytes.Compare(nodes[i].key, nodes[j].key) < 0
	})

	for _, n := range nodes {
		fn(n.hash, n)
	}
}

func (ndb *nodeDB) String() string {
	var str string
	index := 0

	ndb.traversePrefix(rootKeyFormat.Key(), func(key, value []byte) {
		str += fmt.Sprintf("%s: %x\n", string(key), value)
	})
	str += "\n"

	ndb.traverseOrphans(func(key, value []byte) {
		str += fmt.Sprintf("%s: %x\n", string(key), value)
	})
	str += "\n"

	ndb.traverseNodes(func(hash []byte, node *Node) {
		if len(hash) == 0 {
			str += fmt.Sprintf("<nil>\n")
		} else if node == nil {
			str += fmt.Sprintf("%s%40x: <nil>\n", nodeKeyFormat.Prefix(), hash)
		} else if node.value == nil && node.height > 0 {
			str += fmt.Sprintf("%s%40x: %s   %-16s h=%d version=%d\n",
				nodeKeyFormat.Prefix(), hash, node.key, "", node.height, node.version)
		} else {
			str += fmt.Sprintf("%s%40x: %s = %-16s h=%d version=%d\n",
				nodeKeyFormat.Prefix(), hash, node.key, node.value, node.height, node.version)
		}
		index++
	})
	return "-" + "\n" + str + "-"
}
