package iavl

import (
	"bytes"
	"container/list"
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
	mtx           sync.Mutex // Read/write lock.
	snapshotDB    dbm.DB     // Persistent node storage.
	recentDB      dbm.DB     // Memory node storage.
	snapshotBatch dbm.Batch  // Batched writing buffer.
	recentBatch   dbm.Batch  // Batched writing buffer for recentDB.
	keepRecent    int64      // number of recent versions to store in recentDB
	keepEvery     int64      // frequency of snaphots saved to disk

	latestVersion  int64
	nodeCache      map[string]*list.Element // Node cache.
	nodeCacheSize  int                      // Node cache size limit in elements.
	nodeCacheQueue *list.List               // LRU queue of cache elements. Used for deletion.
}

func newNodeDB(snapshotDB dbm.DB, recentDB dbm.DB, cacheSize int, keepEvery, keepRecent int64) *nodeDB {
	ndb := &nodeDB{
		snapshotDB:     snapshotDB,
		recentDB:       recentDB,
		snapshotBatch:  snapshotDB.NewBatch(),
		recentBatch:    recentDB.NewBatch(),
		keepRecent:     keepRecent,
		keepEvery:      keepEvery,
		latestVersion:  0, // initially invalid
		nodeCache:      make(map[string]*list.Element),
		nodeCacheSize:  cacheSize,
		nodeCacheQueue: list.New(),
	}
	return ndb
}

func (ndb *nodeDB) isSnapshotVersion(version int64) bool {
	return ndb.keepEvery != 0 && version%ndb.keepEvery == 0
}

func (ndb *nodeDB) isRecentVersion(version int64) bool {
	return ndb.keepRecent != 0 && version > ndb.latestVersion-ndb.keepRecent
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

	//Try reading from recent memDB
	buf := ndb.recentDB.Get(ndb.nodeKey(hash))
	persisted := false
	if buf == nil {
		// Doesn't exist, load from disk
		buf = ndb.snapshotDB.Get(ndb.nodeKey(hash))
		if buf == nil {
			panic(fmt.Sprintf("Value missing for hash %x corresponding to nodeKey %s", hash, ndb.nodeKey(hash)))
		}
		persisted = true
	}

	node, err := MakeNode(buf)
	if err != nil {
		panic(fmt.Sprintf("Error reading Node. bytes: %x, error: %v", buf, err))
	}
	node.saved = true
	node.persisted = persisted

	node.hash = hash
	ndb.cacheNode(node)

	return node
}

// SaveNode saves a node to disk.
func (ndb *nodeDB) SaveNode(node *Node, flushToDisk bool) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	if node.hash == nil {
		panic("Expected to find node.hash, but none found.")
	}
	if node.saved && !flushToDisk {
		return
	}
	if node.persisted {
		panic("Shouldn't be calling save on an already persisted node.")
	}

	// Save node bytes to db.
	buf := new(bytes.Buffer)
	if err := node.writeBytes(buf); err != nil {
		panic(err)
	}

	if flushToDisk {
		ndb.snapshotBatch.Set(ndb.nodeKey(node.hash), buf.Bytes())
		node.persisted = true
	}

	node.saved = true
	ndb.recentBatch.Set(ndb.nodeKey(node.hash), buf.Bytes())
}

// Has checks if a hash exists in the database.
func (ndb *nodeDB) Has(hash []byte) bool {
	key := ndb.nodeKey(hash)

	val := ndb.recentDB.Get(key)
	if val != nil {
		return true
	}

	if ldb, ok := ndb.snapshotDB.(*dbm.GoLevelDB); ok {
		exists, err := ldb.DB().Has(key, nil)
		if err != nil {
			panic("Got error from leveldb: " + err.Error())
		}
		return exists
	}
	return ndb.snapshotDB.Get(key) != nil
}

// SaveTree takes a rootNode and version. Saves all nodes in tree using SaveBranch
func (ndb *nodeDB) SaveTree(root *Node, version int64) []byte {
	flushToDisk := ndb.isSnapshotVersion(version)
	return ndb.SaveBranch(root, flushToDisk)
}

// SaveBranch saves the given node and all of its descendants.
// NOTE: This function clears leftNode/rigthNode recursively and
// calls _hash() on the given node.
// TODO refactor, maybe use hashWithCount() but provide a callback.
func (ndb *nodeDB) SaveBranch(node *Node, flushToDisk bool) []byte {
	if node.saved {
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
	ndb.deleteVersion(version, checkLatestVersion, false)
}

func (ndb *nodeDB) DeleteVersionFromRecent(version int64, checkLatestVersion bool) {
	ndb.deleteVersion(version, checkLatestVersion, true)
}

func (ndb *nodeDB) deleteVersion(version int64, checkLatestVersion, memOnly bool) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	ndb.deleteOrphans(version, memOnly)
	ndb.deleteRoot(version, checkLatestVersion, memOnly)
}

// Saves orphaned nodes to disk under a special prefix.
// version: the new version being saved.
// orphans: the orphan nodes created since version-1
func (ndb *nodeDB) SaveOrphans(version int64, orphans map[string]int64) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	toVersion := ndb.getPreviousVersion(version)

	for hash, fromVersion := range orphans {
		debug("SAVEORPHAN %v-%v %X\n", fromVersion, toVersion, hash)
		// if snapshot version in between fromVersion and toVersion, then flush to disk. Or fromVersion == toVersion == snapshotVersion
		flushToDisk := fromVersion/ndb.keepEvery != toVersion/ndb.keepEvery || (ndb.isSnapshotVersion(toVersion) && ndb.isSnapshotVersion(fromVersion))
		ndb.saveOrphan([]byte(hash), fromVersion, toVersion, flushToDisk)
	}
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
		snapVersion := toVersion - (toVersion % ndb.keepEvery)
		key := ndb.orphanKey(fromVersion, snapVersion, hash)
		ndb.snapshotBatch.Set(key, hash)
	}
}

// deleteOrphans deletes orphaned nodes from disk, and the associated orphan
// entries.
func (ndb *nodeDB) deleteOrphans(version int64, memOnly bool) {
	if ndb.keepRecent != 0 {
		ndb.deleteOrphansMem(version)
	}
	if ndb.isSnapshotVersion(version) && !memOnly {
		traverseOrphansVersionFromDB(ndb.snapshotDB, version, func(key, hash []byte) {
			ndb.snapshotDB.Delete(key)
			ndb.deleteOrphansHelper(ndb.snapshotDB, ndb.snapshotBatch, true, key, hash)
		})
	}
}

func (ndb *nodeDB) deleteOrphansMem(version int64) {
	traverseOrphansVersionFromDB(ndb.recentDB, version, func(key, hash []byte) {
		if ndb.keepRecent == 0 {
			return
		}
		ndb.recentBatch.Delete(key)
		// common case, we are deleting orphans from least recent version that is getting pruned from memDB
		if version == ndb.latestVersion-ndb.keepRecent {
			// delete orphan look-up, delete and uncache node
			ndb.recentBatch.Delete(ndb.nodeKey(hash))
			ndb.uncacheNode(hash)
			return
		}

		// user is manually deleting version from memDB
		// thus predecessor may exist in memDB
		// Will be zero if there is no previous version.
		ndb.deleteOrphansHelper(ndb.recentDB, ndb.recentBatch, false, key, hash)
	})
}

func (ndb *nodeDB) deleteOrphansHelper(db dbm.DB, batch dbm.Batch, flushToDisk bool, key, hash []byte) {
	var fromVersion, toVersion int64

	// See comment on `orphanKeyFmt`. Note that here, `version` and
	// `toVersion` are always equal.
	orphanKeyFormat.Scan(key, &toVersion, &fromVersion)

	predecessor := getPreviousVersionFromDB(toVersion, db)
	// If there is no predecessor, or the predecessor is earlier than the
	// beginning of the lifetime (ie: negative lifetime), or the lifetime
	// spans a single version and that version is the one being deleted, we
	// can delete the orphan.  Otherwise, we shorten its lifetime, by
	// moving its endpoint to the previous version.
	if predecessor < fromVersion || fromVersion == toVersion {
		debug("DELETE predecessor:%v fromVersion:%v toVersion:%v %X\n", predecessor, fromVersion, toVersion, hash)
		batch.Delete(ndb.nodeKey(hash))
		ndb.uncacheNode(hash)
	} else {
		debug("MOVE predecessor:%v fromVersion:%v toVersion:%v %X\n", predecessor, fromVersion, toVersion, hash)
		ndb.saveOrphan(hash, fromVersion, predecessor, flushToDisk)
	}
}

func (ndb *nodeDB) PruneRecentVersions() (prunedVersions []int64) {
	if ndb.keepRecent == 0 || ndb.latestVersion-ndb.keepRecent <= 0 {
		return nil
	}
	pruneVer := ndb.latestVersion - ndb.keepRecent
	ndb.DeleteVersionFromRecent(pruneVer, true)
	return append(prunedVersions, pruneVer)
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
	// If version exists in recentDB, check recentDB for any previous version
	if ndb.isRecentVersion(version) {
		prev := getPreviousVersionFromDB(version, ndb.recentDB)
		if prev != 0 {
			return prev
		}
	}
	return getPreviousVersionFromDB(version, ndb.snapshotDB)
}

func getPreviousVersionFromDB(version int64, db dbm.DB) int64 {
	itr := db.ReverseIterator(
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
func (ndb *nodeDB) deleteRoot(version int64, checkLatestVersion, memOnly bool) {
	if checkLatestVersion && version == ndb.getLatestVersion() {
		panic("Tried to delete latest version")
	}

	key := ndb.rootKey(version)
	ndb.recentBatch.Delete(key)
	if ndb.isSnapshotVersion(version) && !memOnly {
		ndb.snapshotBatch.Delete(key)
	}
}

func (ndb *nodeDB) traverseOrphans(fn func(k, v []byte)) {
	ndb.traversePrefix(orphanKeyFormat.Key(), fn)
}

// Traverse orphans ending at a certain version.
// NOTE: If orphan is in recentDB and levelDB (version > latestVersion-keepRecent && version%keepEvery == 0)
// traverse will return the node twice.
func (ndb *nodeDB) traverseOrphansVersion(version int64, fn func(k, v []byte)) {
	prefix := orphanKeyFormat.Key(version)
	if ndb.isRecentVersion(version) {
		traversePrefixFromDB(ndb.recentDB, prefix, fn)
	}
	if ndb.isSnapshotVersion(version) {
		traversePrefixFromDB(ndb.snapshotDB, prefix, fn)
	}
}

func traverseOrphansVersionFromDB(db dbm.DB, version int64, fn func(k, v []byte)) {
	prefix := orphanKeyFormat.Key(version)
	traversePrefixFromDB(db, prefix, fn)
}

// Traverse all keys from recentDB and disk DB
func (ndb *nodeDB) traverse(fn func(key, value []byte)) {
	memItr := ndb.recentDB.Iterator(nil, nil)
	defer memItr.Close()

	for ; memItr.Valid(); memItr.Next() {
		fn(memItr.Key(), memItr.Value())
	}

	itr := ndb.snapshotDB.Iterator(nil, nil)
	defer itr.Close()

	for ; itr.Valid(); itr.Next() {
		fn(itr.Key(), itr.Value())
	}
}

// Traverse all keys from provided DB
func traverseFromDB(db dbm.DB, fn func(key, value []byte)) {
	itr := db.Iterator(nil, nil)
	defer itr.Close()

	for ; itr.Valid(); itr.Next() {
		fn(itr.Key(), itr.Value())
	}
}

// Traverse all keys with a certain prefix from recentDB and disk DB
func (ndb *nodeDB) traversePrefix(prefix []byte, fn func(k, v []byte)) {
	memItr := dbm.IteratePrefix(ndb.recentDB, prefix)
	defer memItr.Close()

	for ; memItr.Valid(); memItr.Next() {
		fn(memItr.Key(), memItr.Value())
	}

	itr := dbm.IteratePrefix(ndb.snapshotDB, prefix)
	defer itr.Close()

	for ; itr.Valid(); itr.Next() {
		fn(itr.Key(), itr.Value())
	}
}

// Traverse all keys with a certain prefix from given DB
func traversePrefixFromDB(db dbm.DB, prefix []byte, fn func(k, v []byte)) {
	itr := dbm.IteratePrefix(db, prefix)
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
func (ndb *nodeDB) Commit() {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	if ndb.keepEvery != 0 {
		ndb.snapshotBatch.Write()
		ndb.snapshotBatch.Close()
	}
	if ndb.keepRecent != 0 {
		ndb.recentBatch.Write()
		ndb.recentBatch.Close()
	}
	ndb.snapshotBatch = ndb.snapshotDB.NewBatch()
	ndb.recentBatch = ndb.recentDB.NewBatch()
}

func (ndb *nodeDB) getRoot(version int64) []byte {
	if ndb.isRecentVersion(version) {
		memroot := ndb.recentDB.Get(ndb.rootKey(version))
		// TODO: maybe I shouldn't check in snapshot if it isn't here
		if len(memroot) > 0 {
			return memroot
		}
	}

	return ndb.snapshotDB.Get(ndb.rootKey(version))
}

// NOTE: will get duplicated root if snapshot version exists in DB and memDB
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
	return ndb.saveRoot(root.hash, version, ndb.isSnapshotVersion(version))
}

// SaveEmptyRoot creates an entry on disk for an empty root.
func (ndb *nodeDB) SaveEmptyRoot(version int64) error {
	return ndb.saveRoot([]byte{}, version, ndb.isSnapshotVersion(version))
}

func (ndb *nodeDB) saveRoot(hash []byte, version int64, flushToDisk bool) error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	if version != ndb.getLatestVersion()+1 {
		return fmt.Errorf("Must save consecutive versions. Expected %d, got %d", ndb.getLatestVersion()+1, version)
	}

	key := ndb.rootKey(version)
	ndb.updateLatestVersion(version)
	ndb.recentBatch.Set(key, hash)
	if flushToDisk {
		ndb.snapshotBatch.Set(key, hash)
	}

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
