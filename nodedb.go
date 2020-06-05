package iavl

import (
	"bytes"
	"container/list"
	"fmt"
	"sort"
	"sync"

	lru "github.com/hashicorp/golang-lru"
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

	metadataKeyFormat = NewKeyFormat('m', int64Size) // m<version>
)

type nodeDB struct {
	mtx            sync.Mutex       // Read/write lock.
	snapshotDB     dbm.DB           // Persistent node storage.
	recentDB       dbm.DB           // Memory node storage.
	snapshotBatch  dbm.Batch        // Batched writing buffer.
	recentBatch    dbm.Batch        // Batched writing buffer for recentDB.
	opts           *Options         // Options to customize for pruning/writing
	versionReaders map[int64]uint32 // Number of active version readers (prevents pruning)

	latestVersion  int64
	nodeCache      map[string]*list.Element // Node cache.
	nodeCacheSize  int                      // Node cache size limit in elements.
	nodeCacheQueue *list.List               // LRU queue of cache elements. Used for deletion.

	vmCache *lru.Cache // LRU cache of version metadata
}

func newNodeDB(snapshotDB dbm.DB, recentDB dbm.DB, cacheSize int, opts *Options) *nodeDB {
	if opts == nil {
		opts = DefaultOptions()
	}

	vmCache, err := lru.New(20000)
	if err != nil {
		panic(fmt.Errorf("failed to create metadata cache: %w", err))
	}

	return &nodeDB{
		snapshotDB:     snapshotDB,
		recentDB:       recentDB,
		snapshotBatch:  snapshotDB.NewBatch(),
		recentBatch:    recentDB.NewBatch(),
		opts:           opts,
		latestVersion:  0, // initially invalid
		nodeCache:      make(map[string]*list.Element),
		nodeCacheSize:  cacheSize,
		nodeCacheQueue: list.New(),
		versionReaders: make(map[int64]uint32, 8),
		vmCache:        vmCache,
	}
}

// GetVersionMetadata returns a reference to VersionMetadata for a given version.
// The object is first checked against the internal VersionMetadata LRU cache. If
// it does not exist, it will be queried for via the snapshotDB and added to the
// LRU cache. An error is returned if the VersionMetadata cannot be decoded or
// queried for in the snapshotDB.
func (ndb *nodeDB) GetVersionMetadata(version int64) (*VersionMetadata, error) {
	key := metadataKeyFormat.Key(version)

	v, ok := ndb.vmCache.Get(string(key))
	if ok {
		return v.(*VersionMetadata), nil
	}

	bz, err := ndb.snapshotDB.Get(key)
	if err != nil {
		return nil, err
	}

	// In cases where version metadata does not exist, say for a non-empty tree,
	// we construct metadata on the fly. This is for backwards compatibility WRT
	// to snapshotting.
	if len(bz) == 0 {
		return &VersionMetadata{
			Version:  version,
			Snapshot: ndb.opts.KeepEvery != 0 && version%ndb.opts.KeepEvery == 0,
		}, nil
	}

	vm, err := unmarshalVersionMetadata(bz)
	if err != nil {
		return nil, err
	}

	_ = ndb.vmCache.Add(string(key), vm)
	return vm, nil
}

// SetVersionMetadata saves a VersionMetadata in the snapshotDB and performs a
// write-through to the VersionMetadata LRU cache. It returns an error if encoding
// or saving to the snapshotDB fails.
func (ndb *nodeDB) SetVersionMetadata(vm *VersionMetadata) error {
	key := metadataKeyFormat.Key(vm.Version)

	bz, err := vm.marshal()
	if err != nil {
		return err
	}

	if err := ndb.snapshotDB.Set(key, bz); err != nil {
		return err
	}

	_ = ndb.vmCache.Add(string(key), vm)
	return nil
}

// DeleteVersionMetadata removes a VersionMetadata object from the snapshotDB
// and the LRU cache.
func (ndb *nodeDB) DeleteVersionMetadata(version int64) error {
	vm, err := ndb.GetVersionMetadata(version)
	if err != nil {
		return err
	}

	key := metadataKeyFormat.Key(vm.Version)

	if err := ndb.snapshotDB.Delete(key); err != nil {
		return err
	}

	ndb.vmCache.Remove(string(key))
	return nil
}

func (ndb *nodeDB) isRecentVersion(version int64) bool {
	return ndb.opts.KeepRecent != 0 && version > ndb.latestVersion-ndb.opts.KeepRecent
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
	buf, err := ndb.recentDB.Get(ndb.nodeKey(hash))
	if err != nil {
		panic(fmt.Sprintf("can't get node %X: %v", hash, err))
	}
	persisted := false
	if buf == nil {
		// Doesn't exist, load from disk
		buf, err = ndb.snapshotDB.Get(ndb.nodeKey(hash))
		if err != nil {
			panic(err)
		}
		if buf == nil {
			panic(fmt.Sprintf("Value missing for hash %x corresponding to nodeKey %x", hash, ndb.nodeKey(hash)))
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
	ndb.saveNodeBatch(node, flushToDisk, ndb.recentBatch, ndb.snapshotBatch)
}

func (ndb *nodeDB) saveNodeBatch(node *Node, flushToDisk bool, rb, sb dbm.Batch) {
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
	var buf bytes.Buffer
	buf.Grow(node.aminoSize())

	if err := node.writeBytes(&buf); err != nil {
		panic(err)
	}

	if !node.saved {
		node.saved = true
		rb.Set(ndb.nodeKey(node.hash), buf.Bytes())
	}

	if flushToDisk {
		sb.Set(ndb.nodeKey(node.hash), buf.Bytes())
		node.persisted = true
		node.saved = true
	}
}

// Has checks if a hash exists in the recentDB or the snapshotDB.
func (ndb *nodeDB) Has(hash []byte) (bool, error) {
	key := ndb.nodeKey(hash)

	val, err := ndb.recentDB.Get(key)
	if err != nil {
		return false, errors.Wrap(err, "recentDB")
	}
	if val != nil {
		return true, nil
	}

	return ndb.HasSnapshot(hash)
}

// HasSnapshot returns true if a given hash exists in the snapshotDB.
func (ndb *nodeDB) HasSnapshot(hash []byte) (bool, error) {
	key := ndb.nodeKey(hash)
	if ldb, ok := ndb.snapshotDB.(*dbm.GoLevelDB); ok {
		var exists bool

		exists, err := ldb.DB().Has(key, nil)
		if err != nil {
			return false, errors.Wrap(err, "snapshotDB")
		}

		return exists, nil
	}

	value, err := ndb.snapshotDB.Get(key)
	if err != nil {
		return false, errors.Wrap(err, "snapshotDB")
	}

	return value != nil, nil
}

// SaveTree takes a rootNode and version. Saves all nodes in tree using SaveBranch
func (ndb *nodeDB) SaveTree(root *Node, version int64) ([]byte, error) {
	vm, err := ndb.GetVersionMetadata(version)
	if err != nil {
		return nil, err
	}

	return ndb.SaveBranch(root, vm.Snapshot), nil
}

// SaveBranch saves the given node and all of its descendants.
// NOTE: This function clears leftNode/rigthNode recursively and
// calls _hash() on the given node.
// TODO refactor, maybe use hashWithCount() but provide a callback.
func (ndb *nodeDB) SaveBranch(node *Node, flushToDisk bool) []byte {
	return ndb.saveBranchBatch(node, flushToDisk, ndb.recentBatch, ndb.snapshotBatch)
}

// TODO: Reconsider design to not have batch objects be fields of a nodeDB type.
// Instead, batch objects should be created when needed as passed as arguments
// where needed. This allows the IO flow to be easier to reason about and impproves
// testability.
func (ndb *nodeDB) saveBranchBatch(node *Node, flushToDisk bool, rb, sb dbm.Batch) []byte {
	if node.saved && !flushToDisk {
		return node.hash
	}
	if node.persisted {
		return node.hash
	}

	if node.size != 1 {
		if node.leftNode == nil {
			node.leftNode = ndb.GetNode(node.leftHash)
		}
		if node.rightNode == nil {
			node.rightNode = ndb.GetNode(node.rightHash)
		}
		node.leftHash = ndb.saveBranchBatch(node.leftNode, flushToDisk, rb, sb)
		node.rightHash = ndb.saveBranchBatch(node.rightNode, flushToDisk, rb, sb)
	}

	node._hash()
	ndb.saveNodeBatch(node, flushToDisk, rb, sb)

	node.leftNode = nil
	node.rightNode = nil

	return node.hash
}

// DeleteVersion deletes a tree version from disk.
func (ndb *nodeDB) DeleteVersion(version int64, checkLatestVersion bool) error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	return ndb.deleteVersion(version, checkLatestVersion, false)
}

func (ndb *nodeDB) DeleteVersionFromRecent(version int64, checkLatestVersion bool) error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	return ndb.deleteVersion(version, checkLatestVersion, true)
}

func (ndb *nodeDB) deleteVersion(version int64, checkLatestVersion, memOnly bool) error {
	if ndb.versionReaders[version] > 0 {
		return errors.Errorf("unable to delete version %v, it has %v active readers", version, ndb.versionReaders[version])
	}

	vm, err := ndb.GetVersionMetadata(version)
	if err != nil {
		return err
	}

	if err := ndb.deleteOrphans(version, memOnly, vm.Snapshot); err != nil {
		return err
	}

	ndb.deleteRoot(version, checkLatestVersion, memOnly, vm.Snapshot)
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

func (ndb *nodeDB) PruneRecentVersion() (int64, error) {
	if ndb.opts.KeepRecent == 0 || ndb.latestVersion-ndb.opts.KeepRecent <= 0 {
		return 0, nil
	}

	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	pruneVer := ndb.latestVersion - ndb.opts.KeepRecent
	if ndb.versionReaders[pruneVer] > 0 {
		return 0, nil
	}

	if err := ndb.deleteVersion(pruneVer, true, true); err != nil {
		return 0, err
	}

	vm, err := ndb.GetVersionMetadata(pruneVer)
	if err != nil {
		return 0, err
	}

	if vm.Snapshot {
		return 0, nil
	}

	return pruneVer, nil
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
	itr, err := db.ReverseIterator(
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
func (ndb *nodeDB) deleteRoot(version int64, checkLatestVersion, memOnly, isSnapshot bool) {
	if checkLatestVersion && version == ndb.getLatestVersion() {
		panic("Tried to delete latest version")
	}

	key := ndb.rootKey(version)
	ndb.recentBatch.Delete(key)

	if isSnapshot && !memOnly {
		ndb.snapshotBatch.Delete(key)
	}
}

func (ndb *nodeDB) traverseOrphans(fn func(k, v []byte)) {
	ndb.traversePrefix(orphanKeyFormat.Key(), fn)
}

func traverseOrphansFromDB(db dbm.DB, fn func(k, v []byte)) {
	traversePrefixFromDB(db, orphanKeyFormat.Key(), fn)
}

// Traverse orphans ending at a certain version.
// NOTE: If orphan is in recentDB and levelDB (version > latestVersion-keepRecent && version%keepEvery == 0)
// traverse will return the node twice.
// func (ndb *nodeDB) traverseOrphansVersion(version int64, fn func(k, v []byte)) {
// 	prefix := orphanKeyFormat.Key(version)
// 	if ndb.isRecentVersion(version) {
// 		traversePrefixFromDB(ndb.recentDB, prefix, fn)
// 	}
// 	if ndb.isSnapshotVersion(version) {
// 		traversePrefixFromDB(ndb.snapshotDB, prefix, fn)
// 	}
// }

func traverseOrphansVersionFromDB(db dbm.DB, version int64, fn func(k, v []byte)) {
	prefix := orphanKeyFormat.Key(version)
	traversePrefixFromDB(db, prefix, fn)
}

// Traverse all keys from recentDB and disk DB
func (ndb *nodeDB) traverse(fn func(key, value []byte)) {
	memItr, err := ndb.recentDB.Iterator(nil, nil)
	if err != nil {
		panic(err)
	}
	defer memItr.Close()

	for ; memItr.Valid(); memItr.Next() {
		fn(memItr.Key(), memItr.Value())
	}

	itr, err := ndb.snapshotDB.Iterator(nil, nil)
	if err != nil {
		panic(err)
	}
	defer itr.Close()

	for ; itr.Valid(); itr.Next() {
		fn(itr.Key(), itr.Value())
	}
}

// Traverse all keys from provided DB
func traverseFromDB(db dbm.DB, fn func(key, value []byte)) {
	itr, err := db.Iterator(nil, nil)
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
	memItr, err := dbm.IteratePrefix(ndb.recentDB, prefix)
	if err != nil {
		panic(err)
	}
	defer memItr.Close()

	for ; memItr.Valid(); memItr.Next() {
		fn(memItr.Key(), memItr.Value())
	}

	itr, err := dbm.IteratePrefix(ndb.snapshotDB, prefix)
	if err != nil {
		panic(err)
	}
	defer itr.Close()

	for ; itr.Valid(); itr.Next() {
		fn(itr.Key(), itr.Value())
	}
}

// Traverse all keys with a certain prefix from given DB
func traversePrefixFromDB(db dbm.DB, prefix []byte, fn func(k, v []byte)) {
	itr, err := dbm.IteratePrefix(db, prefix)
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
	if ndb.isRecentVersion(version) {
		memroot, err := ndb.recentDB.Get(ndb.rootKey(version))
		if err != nil {
			return nil, err
		}
		// TODO: maybe I shouldn't check in snapshot if it isn't here
		if memroot != nil {
			return memroot, nil
		}
	}

	return ndb.snapshotDB.Get(ndb.rootKey(version))
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

	vm, err := ndb.GetVersionMetadata(version)
	if err != nil {
		return err
	}

	return ndb.saveRoot(root.hash, version, vm.Snapshot)
}

// SaveEmptyRoot creates an entry on disk for an empty root.
func (ndb *nodeDB) SaveEmptyRoot(version int64) error {
	vm, err := ndb.GetVersionMetadata(version)
	if err != nil {
		return err
	}

	return ndb.saveRoot([]byte{}, version, vm.Snapshot)
}

func (ndb *nodeDB) saveRoot(hash []byte, version int64, flushToDisk bool) error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	if version != ndb.getLatestVersion()+1 {
		return fmt.Errorf("must save consecutive versions; expected %d, got %d", ndb.getLatestVersion()+1, version)
	}

	key := ndb.rootKey(version)
	ndb.updateLatestVersion(version)
	ndb.recentBatch.Set(key, hash)
	if flushToDisk {
		ndb.snapshotBatch.Set(key, hash)
	}

	return nil
}

func (ndb *nodeDB) saveRootBatch(hash []byte, version int64, rb, sb dbm.Batch) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	key := ndb.rootKey(version)
	rb.Set(key, hash)
	sb.Set(key, hash)
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

func (ndb *nodeDB) flushVersion(version int64) error {
	// Create new recentDB and snapshotDB batch objects. We don't use the current
	// batch objects of the nodeDB as we don't want to write state prematurely or
	// have conflicting state.
	//
	// NOTE: We ignore any write that happen to recentBatch.
	rb := ndb.recentDB.NewBatch()
	defer rb.Close()

	sb := ndb.snapshotDB.NewBatch()
	defer sb.Close()

	rootHash, err := ndb.getRoot(version)
	if err != nil {
		return err
	}

	switch {
	case rootHash == nil:
		return fmt.Errorf("version %v does not exist in recentDB", version)

	case len(rootHash) == 0:
		ndb.saveRootBatch([]byte{}, version, rb, sb)

	default:
		// save branch, the root, and the necessary orphans
		node := ndb.GetNode(rootHash)
		ndb.saveBranchBatch(node, true, rb, sb)
		ndb.saveRootBatch(node.hash, version, rb, sb)
	}

	traverseOrphansVersionFromDB(ndb.recentDB, version, func(k, v []byte) {
		var fromVersion, toVersion int64
		orphanKeyFormat.Scan(k, &toVersion, &fromVersion)

		ndb.saveOrphan(v, fromVersion, toVersion, true)
	})

	if err := sb.WriteSync(); err != nil {
		return fmt.Errorf("failed to write (sync) the snapshot batch: %w", err)
	}

	// NOTE: We do not need to write/flush the recent batch.
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

func (ndb *nodeDB) nodesFromDB(db dbm.DB) []*Node {
	nodes := []*Node{}

	ndb.traverseNodesFromDB(db, func(hash []byte, node *Node) {
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

func (ndb *nodeDB) traverseNodesFromDB(db dbm.DB, fn func(hash []byte, node *Node)) {
	nodes := []*Node{}

	traversePrefixFromDB(db, nodeKeyFormat.Key(), func(key, value []byte) {
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
