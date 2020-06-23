package iavl

import (
	"bytes"
	"container/list"
	"crypto/sha256"
	"fmt"
	"sort"
	"sync"

	"github.com/pkg/errors"
	dbm "github.com/tendermint/tm-db"
)

const (
	int64Size = 8
	hashSize  = sha256.Size
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

	metadataKeyFormat = NewKeyFormat('m', int64Size) // m<version>
)

type nodeDB struct {
	mtx            sync.Mutex       // Read/write lock.
	db             dbm.DB           // Persistent node storage.
	batch          dbm.Batch        // Batched writing buffer.
	opts           *Options         // Options to customize for pruning/writing
	versionReaders map[int64]uint32 // Number of active version readers (prevents pruning)

	latestVersion  int64
	nodeCache      map[string]*list.Element // Node cache.
	nodeCacheSize  int                      // Node cache size limit in elements.
	nodeCacheQueue *list.List               // LRU queue of cache elements. Used for deletion.
}

func newNodeDB(db dbm.DB, cacheSize int, opts *Options) *nodeDB {
	if opts == nil {
		opts = DefaultOptions()
	}
	return &nodeDB{
		db:             db,
		batch:          db.NewBatch(),
		opts:           opts,
		latestVersion:  0, // initially invalid
		nodeCache:      make(map[string]*list.Element),
		nodeCacheSize:  cacheSize,
		nodeCacheQueue: list.New(),
		versionReaders: make(map[int64]uint32, 8),
	}
}

// GetNode gets a node from memory or disk. If it is an inner node, it does not
// load its children.
func (ndb *nodeDB) GetNode(hash []byte) *Node {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	if len(hash) == 0 {
		panic("nodeDB.GetNode() requires hash")
	}

	// Check the cache.
	if elem, ok := ndb.nodeCache[string(hash)]; ok {
		// Already exists. Move to back of nodeCacheQueue.
		ndb.nodeCacheQueue.MoveToBack(elem)
		return elem.Value.(*Node)
	}

	// Doesn't exist, load.
	buf, err := ndb.db.Get(ndb.nodeKey(hash))
	if err != nil {
		panic(fmt.Sprintf("can't get node %X: %v", hash, err))
	}
	if buf == nil {
		panic(fmt.Sprintf("Value missing for hash %x corresponding to nodeKey %x", hash, ndb.nodeKey(hash)))
	}

	node, err := MakeNode(buf)
	if err != nil {
		panic(fmt.Sprintf("Error reading Node. bytes: %x, error: %v", buf, err))
	}

	node.hash = hash
	node.persisted = true
	ndb.cacheNode(node)

	return node
}

// SaveNode saves a node to disk.
func (ndb *nodeDB) SaveNode(node *Node) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	if node.hash == nil {
		panic("Expected to find node.hash, but none found.")
	}
	if node.persisted {
		panic("Shouldn't be calling save on an already persisted node.")
	}

	// Save node bytes to db.
	var buf bytes.Buffer
	buf.Grow(node.aminoSize())

	if err := node.writeBytes(&buf); err != nil {
		panic(err)
	}

	ndb.batch.Set(ndb.nodeKey(node.hash), buf.Bytes())
	debug("BATCH SAVE %X %p\n", node.hash, node)
	node.persisted = true
	ndb.cacheNode(node)
}

// Has checks if a hash exists in the recentDB or the snapshotDB.
func (ndb *nodeDB) Has(hash []byte) (bool, error) {
	key := ndb.nodeKey(hash)
	if ldb, ok := ndb.db.(*dbm.GoLevelDB); ok {
		var exists bool

		exists, err := ldb.DB().Has(key, nil)
		if err != nil {
			return false, errors.Wrap(err, "snapshotDB")
		}

		return exists, nil
	}

	value, err := ndb.db.Get(key)
	if err != nil {
		return false, errors.Wrap(err, "snapshotDB")
	}

	return value != nil, nil
}

// SaveBranch saves the given node and all of its descendants.
// NOTE: This function clears leftNode/rigthNode recursively and
// calls _hash() on the given node.
// TODO refactor, maybe use hashWithCount() but provide a callback.
func (ndb *nodeDB) SaveBranch(node *Node) []byte {
	if node.persisted {
		return node.hash
	}

	if node.leftNode != nil {
		node.leftHash = ndb.SaveBranch(node.leftNode)
	}
	if node.rightNode != nil {
		node.rightHash = ndb.SaveBranch(node.rightNode)
	}

	node._hash()
	ndb.SaveNode(node)

	node.leftNode = nil
	node.rightNode = nil

	return node.hash
}

// DeleteVersion deletes a tree version from disk.
func (ndb *nodeDB) DeleteVersion(version int64, checkLatestVersion bool) error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	return ndb.deleteVersion(version, checkLatestVersion)
}

func (ndb *nodeDB) deleteVersion(version int64, checkLatestVersion bool) error {
	if ndb.versionReaders[version] > 0 {
		return errors.Errorf("unable to delete version %v, it has %v active readers", version, ndb.versionReaders[version])
	}

	if err := ndb.deleteOrphans(version); err != nil {
		return err
	}

	ndb.deleteRoot(version, checkLatestVersion)
	return nil
}

// Saves orphaned nodes to disk under a special prefix.
// version: the new version being saved.
// orphans: the orphan nodes created since version-1
func (ndb *nodeDB) SaveOrphans(version int64, orphans map[string]int64) error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	toVersion := ndb.getPreviousVersion(version)

	for hash, fromVersion := range orphans {
		var flushToDisk bool

		vm, err := ndb.GetVersionMetadata(fromVersion)
		if err != nil {
			return err
		}

		if ndb.opts.KeepEvery != 0 {
			// if snapshot version in between fromVersion and toVersion INCLUSIVE, then flush to disk.
			flushToDisk = fromVersion/ndb.opts.KeepEvery != toVersion/ndb.opts.KeepEvery || vm.Snapshot
		}

		debug("SAVEORPHAN %v-%v %X flushToDisk: %t\n", fromVersion, toVersion, hash, flushToDisk)
		ndb.saveOrphan([]byte(hash), fromVersion, toVersion, flushToDisk)
	}

	return nil
}

// Saves a single orphan to recentDB. If flushToDisk, persist to disk as well.
func (ndb *nodeDB) saveOrphan(hash []byte, fromVersion, toVersion int64, flushToDisk bool) {
	if fromVersion > toVersion {
		panic(fmt.Sprintf("Orphan expires before it comes alive.  %d > %d", fromVersion, toVersion))
	}

	if ndb.isRecentVersion(toVersion) {
		key := ndb.orphanKey(fromVersion, toVersion, hash)
		ndb.recentBatch.Set(key, hash)
	}

	if flushToDisk {
		// save to disk with toVersion equal to snapshotVersion closest to original toVersion
		snapVersion := toVersion - (toVersion % ndb.opts.KeepEvery)
		key := ndb.orphanKey(fromVersion, snapVersion, hash)
		ndb.snapshotBatch.Set(key, hash)
	}
}

// deleteOrphans deletes orphaned nodes from disk, and the associated orphan
// entries.
func (ndb *nodeDB) deleteOrphans(version int64, memOnly, isSnapshot bool) error {
	if ndb.opts.KeepRecent != 0 {
		ndb.deleteOrphansMem(version)
	}

	if isSnapshot && !memOnly {
		predecessor := getPreviousVersionFromDB(version, ndb.snapshotDB)
		traverseOrphansVersionFromDB(ndb.snapshotDB, version, func(key, hash []byte) {
			ndb.snapshotBatch.Delete(key)
			ndb.deleteOrphansHelper(ndb.snapshotDB, ndb.snapshotBatch, true, predecessor, key, hash)
		})
	}

	return nil
}

func (ndb *nodeDB) deleteOrphansMem(version int64) {
	traverseOrphansVersionFromDB(ndb.recentDB, version, func(key, hash []byte) {
		if ndb.opts.KeepRecent == 0 {
			return
		}
		ndb.recentBatch.Delete(key)
		// common case, we are deleting orphans from least recent version that is getting pruned from memDB
		if version == ndb.latestVersion-ndb.opts.KeepRecent {
			// delete orphan look-up, delete and uncache node
			ndb.recentBatch.Delete(ndb.nodeKey(hash))
			ndb.uncacheNode(hash)
			return
		}

		predecessor := getPreviousVersionFromDB(version, ndb.recentDB)

		// user is manually deleting version from memDB
		// thus predecessor may exist in memDB
		// Will be zero if there is no previous version.
		ndb.deleteOrphansHelper(ndb.recentDB, ndb.recentBatch, false, predecessor, key, hash)
	})
}

func (ndb *nodeDB) deleteOrphansHelper(db dbm.DB, batch dbm.Batch, flushToDisk bool, predecessor int64, key, hash []byte) {
	var fromVersion, toVersion int64

	// See comment on `orphanKeyFmt`. Note that here, `version` and
	// `toVersion` are always equal.
	orphanKeyFormat.Scan(key, &toVersion, &fromVersion)

	// If there is no predecessor, or the predecessor is earlier than the
	// beginning of the lifetime (ie: negative lifetime), or the lifetime
	// spans a single version and that version is the one being deleted, we
	// can delete the orphan.  Otherwise, we shorten its lifetime, by
	// moving its endpoint to the previous version.
	if predecessor < fromVersion || fromVersion == toVersion {
		debug("DELETE predecessor:%v fromVersion:%v toVersion:%v %X flushToDisk: %t\n", predecessor, fromVersion, toVersion, hash, flushToDisk)
		batch.Delete(ndb.nodeKey(hash))
		ndb.uncacheNode(hash)
	} else {
		debug("MOVE predecessor:%v fromVersion:%v toVersion:%v %X flushToDisk: %t\n", predecessor, fromVersion, toVersion, hash, flushToDisk)
		ndb.saveOrphan(hash, fromVersion, predecessor, flushToDisk)
	}
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
	itr, err := ndb.db.ReverseIterator(
		rootKeyFormat.Key(1),
		rootKeyFormat.Key(version),
	)
	if err != nil {
		panic(err)
	}
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
	ndb.batch.Delete(ndb.rootKey(version))
}

func (ndb *nodeDB) traverseOrphans(fn func(k, v []byte)) {
	ndb.traversePrefix(orphanKeyFormat.Key(), fn)
}

func traverseOrphansVersionFromDB(db dbm.DB, version int64, fn func(k, v []byte)) {
	prefix := orphanKeyFormat.Key(version)
	traversePrefixFromDB(db, prefix, fn)
}

// Traverse all keys from recentDB and disk DB
func (ndb *nodeDB) traverse(fn func(key, value []byte)) {
	itr, err := ndb.db.Iterator(nil, nil)
	if err != nil {
		panic(err)
	}
	defer itr.Close()

	for ; itr.Valid(); itr.Next() {
		fn(itr.Key(), itr.Value())
	}
}

// Traverse all keys with a certain prefix from recentDB and disk DB
func (ndb *nodeDB) traversePrefix(prefix []byte, fn func(k, v []byte)) {
	itr, err := dbm.IteratePrefix(ndb.db, prefix)
	if err != nil {
		panic(err)
	}
	defer itr.Close()

	for ; itr.Valid(); itr.Next() {
		fn(itr.Key(), itr.Value())
	}
}

func (ndb *nodeDB) uncacheNode(hash []byte) {
	if elem, ok := ndb.nodeCache[string(hash)]; ok {
		ndb.nodeCacheQueue.Remove(elem)
		delete(ndb.nodeCache, string(hash))
	}
}

// Add a node to the cache and pop the least recently used node if we've
// reached the cache size limit.
func (ndb *nodeDB) cacheNode(node *Node) {
	elem := ndb.nodeCacheQueue.PushBack(node)
	ndb.nodeCache[string(node.hash)] = elem

	if ndb.nodeCacheQueue.Len() > ndb.nodeCacheSize {
		oldest := ndb.nodeCacheQueue.Front()
		hash := ndb.nodeCacheQueue.Remove(oldest).(*Node).hash
		delete(ndb.nodeCache, string(hash))
	}
}

// Write to disk and memDB
func (ndb *nodeDB) Commit() error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	var err error
	if ndb.opts.KeepEvery != 0 {
		if ndb.opts.Sync {
			err = ndb.snapshotBatch.WriteSync()
			if err != nil {
				return errors.Wrap(err, "error in snapShotBatch writesync")
			}
		} else {
			err = ndb.snapshotBatch.Write()
			if err != nil {
				return errors.Wrap(err, "error in snapShotBatch write")
			}
		}

		ndb.snapshotBatch.Close()
	}

	if ndb.opts.KeepRecent != 0 {
		if ndb.opts.Sync {
			err = ndb.recentBatch.WriteSync()
			if err != nil {
				return errors.Wrap(err, "error in recentBatch writesync")
			}
		} else {
			err = ndb.recentBatch.Write()
			if err != nil {
				return errors.Wrap(err, "error in recentBatch write")
			}
		}

		ndb.recentBatch.Close()
	}

	ndb.snapshotBatch = ndb.snapshotDB.NewBatch()
	ndb.recentBatch = ndb.recentDB.NewBatch()

	return nil
}

func (ndb *nodeDB) getRoot(version int64) ([]byte, error) {
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
		panic("SaveRoot: root hash should not be empty")
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
		return fmt.Errorf("must save consecutive versions; expected %d, got %d", ndb.getLatestVersion()+1, version)
	}

	ndb.updateLatestVersion(version)
	ndb.batch.Set(ndb.rootKey(version), hash)
	return nil
}

func (ndb *nodeDB) incrVersionReaders(version int64) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	ndb.versionReaders[version]++
}

func (ndb *nodeDB) decrVersionReaders(version int64) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	if ndb.versionReaders[version] > 0 {
		ndb.versionReaders[version]--
	}
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

// restoreNodes restores nodes, which was orphaned, but after overwriting should not be orphans anymore
func (ndb *nodeDB) restoreNodes(version int64) {
	traverseOrphansVersionFromDB(ndb.recentDB, version, func(key, hash []byte) {
		// Delete orphan key and reverse-lookup key.
		ndb.recentBatch.Delete(key)
	})

	traverseOrphansVersionFromDB(ndb.snapshotDB, version, func(key, hash []byte) {
		// Delete orphan key and reverse-lookup key.
		ndb.snapshotBatch.Delete(key)
	})
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
		switch {
		case len(hash) == 0:
			str += "<nil>\n"
		case node == nil:
			str += fmt.Sprintf("%s%40x: <nil>\n", nodeKeyFormat.Prefix(), hash)
		case node.value == nil && node.height > 0:
			str += fmt.Sprintf("%s%40x: %s   %-16s h=%d version=%d\n",
				nodeKeyFormat.Prefix(), hash, node.key, "", node.height, node.version)
		default:
			str += fmt.Sprintf("%s%40x: %s = %-16s h=%d version=%d\n",
				nodeKeyFormat.Prefix(), hash, node.key, node.value, node.height, node.version)
		}
		index++
	})
	return "-" + "\n" + str + "-"
}
