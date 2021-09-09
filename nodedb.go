package iavl

import (
	"bytes"
	"container/list"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/pkg/errors"
	"github.com/tendermint/tendermint/crypto/tmhash"
	dbm "github.com/tendermint/tm-db"
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

	CommitIntervalHeight   int64 = 100
	HeightOrphansCacheSize       = 8
)

type heightOrphansItem struct {
	version  int64
	rootHash []byte
	orphans  []*Node
}

type nodeDB struct {
	mtx            sync.Mutex       // Read/write lock.
	db             dbm.DB           // Persistent node storage.
	batch          dbm.Batch        // Batched writing buffer.
	opts           Options          // Options to customize for pruning/writing
	versionReaders map[int64]uint32 // Number of active version readers

	latestVersion  int64
	nodeCache      map[string]*list.Element // Node cache.
	nodeCacheSize  int                      // Node cache size limit in elements.
	nodeCacheQueue *list.List               // LRU queue of cache elements. Used for deletion.

	orphanNodeCache         map[string]*Node
	heightOrphansCacheQueue *list.List
	heightOrphansCacheSize  int
	heightOrphansMap        map[int64]*heightOrphansItem

	prePersistNodeCache        map[string]*Node
	tempPrePersistNodeCache    map[string]*Node
	tempPrePersistNodeCacheMtx sync.Mutex

	isInitSavedVersion bool
	dbReadCount        int
	nodeReadCount      int
	dbWriteCount       int
}

func newNodeDB(db dbm.DB, cacheSize int, opts *Options) *nodeDB {
	if opts == nil {
		o := DefaultOptions()
		opts = &o
	}
	return &nodeDB{
		db:                      db,
		batch:                   db.NewBatch(),
		opts:                    *opts,
		latestVersion:           0, // initially invalid
		nodeCache:               make(map[string]*list.Element),
		nodeCacheSize:           cacheSize,
		nodeCacheQueue:          list.New(),
		versionReaders:          make(map[int64]uint32, 8),
		orphanNodeCache:         make(map[string]*Node),
		heightOrphansCacheQueue: list.New(),
		heightOrphansCacheSize:  HeightOrphansCacheSize,
		heightOrphansMap:        make(map[int64]*heightOrphansItem),
		prePersistNodeCache:     make(map[string]*Node),
		tempPrePersistNodeCache: make(map[string]*Node),
		isInitSavedVersion:      true,
		dbReadCount:             0,
		dbWriteCount:            0,
	}
}

// GetNode gets a node from memory or disk. If it is an inner node, it does not
// load its children.
func (ndb *nodeDB) GetNode(hash []byte) *Node {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	ndb.addNodeReadCount()
	if len(hash) == 0 {
		panic("nodeDB.GetNode() requires hash")
	}
	if elem, ok := ndb.prePersistNodeCache[string(hash)]; ok {
		return elem
	}
	// Check the cache.
	if elem, ok := ndb.nodeCache[string(hash)]; ok {
		// Already exists. Move to back of nodeCacheQueue.
		ndb.nodeCacheQueue.MoveToBack(elem)
		return elem.Value.(*Node)
	}
	if elem, ok := ndb.orphanNodeCache[string(hash)]; ok {
		return elem
	}
	if elem, ok := ndb.tempPrePersistNodeCache[string(hash)]; ok {
		return elem
	}

	// Doesn't exist, load.
	buf, err := ndb.db.Get(ndb.nodeKey(hash))
	ndb.addDBReadCount()
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

//// SaveNode saves a node to disk.
//func (ndb *nodeDB) SaveNode(node *Node) {
//	ndb.mtx.Lock()
//	defer ndb.mtx.Unlock()
//
//	if node.hash == nil {
//		panic("Expected to find node.hash, but none found.")
//	}
//	if node.persisted {
//		panic("Shouldn't be calling save on an already persisted node.")
//	}
//
//	// Save node bytes to db.
//	var buf bytes.Buffer
//	buf.Grow(node.aminoSize())
//
//	if err := node.writeBytes(&buf); err != nil {
//		panic(err)
//	}
//
//	ndb.batch.Set(ndb.nodeKey(node.hash), buf.Bytes())
//	debug("BATCH SAVE %X %p\n", node.hash, node)
//	node.persisted = true
//	ndb.addDBWriteCount()
//	ndb.cacheNode(node)
//}

// Has checks if a hash exists in the database.
func (ndb *nodeDB) Has(hash []byte) (bool, error) {
	key := ndb.nodeKey(hash)

	if ldb, ok := ndb.db.(*dbm.GoLevelDB); ok {
		exists, err := ldb.DB().Has(key, nil)
		if err != nil {
			return false, err
		}
		return exists, nil
	}
	value, err := ndb.db.Get(key)
	if err != nil {
		return false, err
	}

	return value != nil, nil
}

// DeleteVersion deletes a tree version from disk.
func (ndb *nodeDB) DeleteVersion(version int64, checkLatestVersion bool) error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	if ndb.versionReaders[version] > 0 {
		return errors.Errorf("unable to delete version %v, it has %v active readers", version, ndb.versionReaders[version])
	}

	ndb.deleteOrphans(version)
	ndb.deleteRoot(version, checkLatestVersion)
	return nil
}

// DeleteVersionsFrom permanently deletes all tree versions from the given version upwards.
func (ndb *nodeDB) DeleteVersionsFrom(version int64) error {
	latest := ndb.getLatestVersion()
	if latest < version {
		return nil
	}
	root, err := ndb.getRoot(latest)
	if err != nil {
		return err
	}
	if root == nil {
		return errors.Errorf("root for version %v not found", latest)
	}

	for v, r := range ndb.versionReaders {
		if v >= version && r != 0 {
			return errors.Errorf("unable to delete version %v with %v active readers", v, r)
		}
	}

	// First, delete all active nodes in the current (latest) version whose node version is after
	// the given version.
	err = ndb.deleteNodesFrom(version, root)
	if err != nil {
		return err
	}

	// Next, delete orphans:
	// - Delete orphan entries *and referred nodes* with fromVersion >= version
	// - Delete orphan entries with toVersion >= version-1 (since orphans at latest are not orphans)
	ndb.traverseOrphans(func(key, hash []byte) {
		var fromVersion, toVersion int64
		orphanKeyFormat.Scan(key, &toVersion, &fromVersion)

		if fromVersion >= version {
			ndb.batch.Delete(key)
			ndb.batch.Delete(ndb.nodeKey(hash))
			ndb.uncacheNode(hash)
		} else if toVersion >= version-1 {
			ndb.batch.Delete(key)
		}
	})

	// Finally, delete the version root entries
	ndb.traverseRange(rootKeyFormat.Key(version), rootKeyFormat.Key(int64(math.MaxInt64)), func(k, v []byte) {
		ndb.batch.Delete(k)
	})

	return nil
}

// DeleteVersionsRange deletes versions from an interval (not inclusive).
func (ndb *nodeDB) DeleteVersionsRange(fromVersion, toVersion int64) error {
	if fromVersion >= toVersion {
		return errors.New("toVersion must be greater than fromVersion")
	}
	if toVersion == 0 {
		return errors.New("toVersion must be greater than 0")
	}

	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	latest := ndb.getLatestVersion()
	if latest < toVersion {
		return errors.Errorf("cannot delete latest saved version (%d)", latest)
	}

	predecessor := ndb.getPreviousVersion(fromVersion)

	for v, r := range ndb.versionReaders {
		if v < toVersion && v > predecessor && r != 0 {
			return errors.Errorf("unable to delete version %v with %v active readers", v, r)
		}
	}

	// If the predecessor is earlier than the beginning of the lifetime, we can delete the orphan.
	// Otherwise, we shorten its lifetime, by moving its endpoint to the predecessor version.
	for version := fromVersion; version < toVersion; version++ {
		ndb.traverseOrphansVersion(version, func(key, hash []byte) {
			var from, to int64
			orphanKeyFormat.Scan(key, &to, &from)
			ndb.batch.Delete(key)
			if from > predecessor {
				ndb.batch.Delete(ndb.nodeKey(hash))
				ndb.uncacheNode(hash)
			} else {
				ndb.saveOrphan(hash, from, predecessor)
			}
		})
	}

	// Delete the version root entries
	ndb.traverseRange(rootKeyFormat.Key(fromVersion), rootKeyFormat.Key(toVersion), func(k, v []byte) {
		ndb.batch.Delete(k)
	})

	return nil
}

// deleteNodesFrom deletes the given node and any descendants that have versions after the given
// (inclusive). It is mainly used via LoadVersionForOverwriting, to delete the current version.
func (ndb *nodeDB) deleteNodesFrom(version int64, hash []byte) error {
	if len(hash) == 0 {
		return nil
	}

	node := ndb.GetNode(hash)
	if node.leftHash != nil {
		if err := ndb.deleteNodesFrom(version, node.leftHash); err != nil {
			return err
		}
	}
	if node.rightHash != nil {
		if err := ndb.deleteNodesFrom(version, node.rightHash); err != nil {
			return err
		}
	}

	if node.version >= version {
		ndb.batch.Delete(ndb.nodeKey(hash))
		ndb.uncacheNode(hash)
	}

	return nil
}

//// Saves orphaned nodes to disk under a special prefix.
//// version: the new version being saved.
//// orphans: the orphan nodes created since version-1
//func (ndb *nodeDB) SaveOrphans(version int64, orphans map[string]int64) {
//	ndb.mtx.Lock()
//	defer ndb.mtx.Unlock()
//
//	toVersion := ndb.getPreviousVersion(version)
//	for hash, fromVersion := range orphans {
//		debug("SAVEORPHAN %v-%v %X\n", fromVersion, toVersion, hash)
//		ndb.saveOrphan([]byte(hash), fromVersion, toVersion)
//	}
//}

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
			ndb.uncacheNode(hash)
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

// Traverse orphans ending at a certain version.
func (ndb *nodeDB) traverseOrphansVersion(version int64, fn func(k, v []byte)) {
	ndb.traversePrefix(orphanKeyFormat.Key(version), fn)
}

// Traverse all keys.
func (ndb *nodeDB) traverse(fn func(key, value []byte)) {
	ndb.traverseRange(nil, nil, fn)
}

// Traverse all keys between a given range (excluding end).
func (ndb *nodeDB) traverseRange(start []byte, end []byte, fn func(k, v []byte)) {
	itr, err := ndb.db.Iterator(start, end)
	if err != nil {
		panic(err)
	}
	defer itr.Close()

	for ; itr.Valid(); itr.Next() {
		fn(itr.Key(), itr.Value())
	}
}

// Traverse all keys with a certain prefix.
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

// Write to disk.
func (ndb *nodeDB) Commit(batch dbm.Batch) error {
	var err error
	if ndb.opts.Sync {
		err = batch.WriteSync()
	} else {
		err = batch.Write()
	}
	if err != nil {
		return errors.Wrap(err, "failed to write batch")
	}

	batch.Close()


	return nil
}

func (ndb *nodeDB) getRoot(version int64) ([]byte, error) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	orphansObj := ndb.heightOrphansMap[version]
	if orphansObj != nil {
		return orphansObj.rootHash, nil
	}
	return ndb.db.Get(ndb.rootKey(version))
}

func (ndb *nodeDB) getRoots() (map[int64][]byte, error) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	roots := map[int64][]byte{}

	ndb.traversePrefix(rootKeyFormat.Key(), func(k, v []byte) {
		var version int64
		rootKeyFormat.Scan(k, &version)
		roots[version] = v
	})
	for version, item := range ndb.heightOrphansMap {
		roots[version] = item.rootHash
	}
	return roots, nil
}

// SaveRoot creates an entry on disk for the given root, so that it can be
// loaded later.
func (ndb *nodeDB) SaveRoot(root *Node, version int64, breakSave bool) error {
	if len(root.hash) == 0 {
		panic("SaveRoot: root hash should not be empty")
	}
	return ndb.saveRoot(root.hash, version, breakSave)
}

// SaveEmptyRoot creates an entry on disk for an empty root.
func (ndb *nodeDB) SaveEmptyRoot(version int64, breakSave bool) error {
	return ndb.saveRoot([]byte{}, version, breakSave)
}

func (ndb *nodeDB) saveRoot(hash []byte, version int64, breakSave bool) error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	// We allow the initial version to be arbitrary
	latest := ndb.getLatestVersion()
	if latest > 0 && version != latest + CommitIntervalHeight && (!ndb.isInitSavedVersion) && (!breakSave){
		return fmt.Errorf("must save consecutive versions; expected %d, got %d", latest + CommitIntervalHeight, version)
	}
	ndb.batch.Set(ndb.rootKey(version), hash)
	err := ndb.batch.Write()
	if err != nil {
		return err
	}
	ndb.batch.Close()
	ndb.batch = ndb.db.NewBatch()
	ndb.updateLatestVersion(version)

	ndb.isInitSavedVersion = false
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

// Utility and test functions

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

func (ndb *nodeDB) UpdateBranch(node *Node) []byte {
	if node.persisted || node.prePersisted {
		return node.hash
	}

	if node.leftNode != nil {
		node.leftHash = ndb.UpdateBranch(node.leftNode)
	}
	if node.rightNode != nil {
		node.rightHash = ndb.UpdateBranch(node.rightNode)
	}

	node._hash()
	ndb.SaveNodeToPrePersistCache(node)

	node.leftNode = nil
	node.rightNode = nil
	return node.hash
}

func (ndb *nodeDB) SaveOrphans(version int64, orphans []*Node) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	version = version - 1

	orphansObj := ndb.heightOrphansMap[version]
	if orphansObj != nil {
		orphansObj.orphans = orphans
	}
	for _, node := range orphans {
		ndb.orphanNodeCache[string(node.hash)] = node
		ndb.uncacheNode(node.hash)
		delete(ndb.prePersistNodeCache, string(node.hash))
		node.leftNode = nil
		node.rightNode = nil
	}
}

func (ndb *nodeDB) SetHeightOrphansItem(version int64, rootHash []byte, versionMap map[int64]bool) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	if rootHash == nil {
		rootHash = []byte{}
	}
	orphanObj := &heightOrphansItem{
		version:  version,
		rootHash: rootHash,
	}
	ndb.heightOrphansCacheQueue.PushBack(orphanObj)
	ndb.heightOrphansMap[version] = orphanObj

	for ndb.heightOrphansCacheQueue.Len() > ndb.heightOrphansCacheSize {
		orphans := ndb.heightOrphansCacheQueue.Front()
		oldHeightOrphanItem := ndb.heightOrphansCacheQueue.Remove(orphans).(*heightOrphansItem)
		for _, node := range oldHeightOrphanItem.orphans {
			delete(ndb.orphanNodeCache, string(node.hash))
		}
		delete(ndb.heightOrphansMap, oldHeightOrphanItem.version)
		delete(versionMap, oldHeightOrphanItem.version)
	}
}

func (ndb *nodeDB) uncacheOrphanNode(hash []byte) {
	if _, ok := ndb.orphanNodeCache[string(hash)]; ok {
		delete(ndb.orphanNodeCache, string(hash))
	}
}

func (ndb *nodeDB) SaveNodeToPrePersistCache(node *Node) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	if node.hash == nil {
		panic("Expected to find node.hash, but none found.")
	}
	if node.persisted || node.prePersisted {
		panic("Shouldn't be calling save on an already persisted node.")
	}

	node.prePersisted = true
	ndb.prePersistNodeCache[string(node.hash)] = node
}

func (ndb *nodeDB) BatchSetPrePersistCache() dbm.Batch {
	batch := ndb.db.NewBatch()
	for _, node := range ndb.tempPrePersistNodeCache {
		if node.persisted || (!node.prePersisted) {
			panic("unexpected logic")
		}
		ndb.BatchSet(node, batch)
	}
	return batch
}

func (ndb *nodeDB) SaveNodeFromPrePersistNodeCacheToNodeCache() {
	for _, node := range ndb.tempPrePersistNodeCache {
		if !node.persisted {
			panic("unexpected logic")
		}
		ndb.mtx.Lock()
		ndb.cacheNode(node)
		ndb.mtx.Unlock()
	}
	ndb.tempPrePersistNodeCache = make(map[string]*Node)
}



// SaveNode saves a node to disk.
func (ndb *nodeDB) BatchSet(node *Node, batch dbm.Batch) {
	//ndb.mtx.Lock()
	//defer ndb.mtx.Unlock()

	if node.hash == nil {
		panic("Expected to find node.hash, but none found.")
	}
	if node.persisted {
		panic("Shouldn't be calling save on an already persisted node.")
	}

	if !node.prePersisted {
		panic("Should be calling save on an prePersisted node.")
	}

	// Save node bytes to db.
	var buf bytes.Buffer
	buf.Grow(node.aminoSize())

	if err := node.writeBytes(&buf); err != nil {
		panic(err)
	}

	batch.Set(ndb.nodeKey(node.hash), buf.Bytes())
	debug("BATCH SAVE %X %p\n", node.hash, node)
	node.persisted = true
	ndb.addDBWriteCount()
}

func (ndb *nodeDB) MovePrePersistCacheToTempCache() {
	ndb.tempPrePersistNodeCacheMtx.Lock()
	ndb.mtx.Lock()
	ndb.tempPrePersistNodeCache = ndb.prePersistNodeCache
	ndb.prePersistNodeCache = map[string]*Node{}
	ndb.mtx.Unlock()
	ndb.tempPrePersistNodeCacheMtx.Unlock()
}

func (ndb *nodeDB) addDBReadCount() {
	ndb.dbReadCount ++
}

func (ndb *nodeDB) addDBWriteCount() {
	ndb.dbWriteCount ++
}

func (ndb *nodeDB) addNodeReadCount() {
	ndb.nodeReadCount ++
}

func (ndb *nodeDB) resetDBReadCount() {
	ndb.dbReadCount = 0
}

func (ndb *nodeDB) resetDBWriteCount() {
	ndb.dbWriteCount = 0
}

func (ndb *nodeDB) resetNodeReadCount() {
	ndb.nodeReadCount = 0
}

func (ndb *nodeDB) GetDBReadCount() int {
	return ndb.dbReadCount
}

func (ndb *nodeDB) GetDBWriteCount() int {
	return ndb.dbWriteCount
}

func (ndb *nodeDB) GetNodeReadCount() int {
	return ndb.nodeReadCount
}

func (ndb *nodeDB) PrintCacheLog(version int64) {
	nodeReadCount := ndb.GetNodeReadCount()
	cacheReadCount := ndb.GetNodeReadCount() - ndb.GetDBReadCount()
	printLog := fmt.Sprintf("db prefix: %s", ParseDBName(ndb.db))
	printLog += fmt.Sprintf(" version: %d", version)
	printLog += fmt.Sprintf(", nodeCacheSize: %d", len(ndb.nodeCache))
	printLog += fmt.Sprintf(", orphansNodeCacheSize: %d", len(ndb.orphanNodeCache))
	printLog += fmt.Sprintf(", prePersistNodeCacheSize: %d", len(ndb.prePersistNodeCache))
	printLog += fmt.Sprintf(", tempPrePersistNodeCacheSize %d", len(ndb.tempPrePersistNodeCache))
	printLog += fmt.Sprintf(", dbReadCount: %d", ndb.GetDBReadCount())
	printLog += fmt.Sprintf(", dbWriteCount: %d", ndb.GetDBWriteCount())

	if nodeReadCount > 0 {
		printLog += fmt.Sprintf(", cacheHitRate: %.2f%%", float64(cacheReadCount) / float64(nodeReadCount) * 100)
	} else {
		printLog += fmt.Sprintf(", cacheHitRate: 0 read")
	}
	fmt.Println(printLog)
	ndb.resetDBReadCount()
	ndb.resetDBWriteCount()
	ndb.resetNodeReadCount()
}
