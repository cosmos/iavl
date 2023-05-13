package iavl

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"

	"cosmossdk.io/log"
	dbm "github.com/cosmos/cosmos-db"

	"github.com/cosmos/iavl/cache"
	"github.com/cosmos/iavl/fastnode"
	"github.com/cosmos/iavl/keyformat"
)

const (
	int32Size         = 4
	int64Size         = 8
	hashSize          = sha256.Size
	genesisVersion    = 1
	storageVersionKey = "storage_version"
	// We store latest saved version together with storage version delimited by the constant below.
	// This delimiter is valid only if fast storage is enabled (i.e. storageVersion >= fastStorageVersionValue).
	// The latest saved version is needed for protection against downgrade and re-upgrade. In such a case, it would
	// be possible to observe mismatch between the latest version state and the fast nodes on disk.
	// Therefore, we would like to detect that and overwrite fast nodes on disk with the latest version state.
	fastStorageVersionDelimiter = "-"
	// Using semantic versioning: https://semver.org/
	defaultStorageVersionValue = "1.0.0"
	fastStorageVersionValue    = "1.1.0"
	fastNodeCacheSize          = 100000
)

var (
	// All node keys are prefixed with the byte 'n'. This ensures no collision is
	// possible with the other keys, and makes them easier to traverse. They are indexed by the version and the local nonce.
	nodeKeyFormat = keyformat.NewKeyFormat('n', int64Size, int32Size) // n<version><nonce>

	// Key Format for making reads and iterates go through a data-locality preserving db.
	// The value at an entry will list what version it was written to.
	// Then to query values, you first query state via this fast method.
	// If its present, then check the tree version. If tree version >= result_version,
	// return result_version. Else, go through old (slow) IAVL get method that walks through tree.
	fastKeyFormat = keyformat.NewKeyFormat('f', 0) // f<keystring>

	// Key Format for storing metadata about the chain such as the version number.
	// The value at an entry will be in a variable format and up to the caller to
	// decide how to parse.
	metadataKeyFormat = keyformat.NewKeyFormat('m', 0) // m<keystring>
)

var errInvalidFastStorageVersion = fmt.Errorf("Fast storage version must be in the format <storage version>%s<latest fast cache version>", fastStorageVersionDelimiter)

type nodeDB struct {
	logger log.Logger

	mtx            sync.Mutex       // Read/write lock.
	db             dbm.DB           // Persistent node storage.
	batch          dbm.Batch        // Batched writing buffer.
	opts           Options          // Options to customize for pruning/writing
	versionReaders map[int64]uint32 // Number of active version readers
	storageVersion string           // Storage version
	firstVersion   int64            // First version of nodeDB.
	latestVersion  int64            // Latest version of nodeDB.
	nodeCache      cache.Cache      // Cache for nodes in the regular tree that consists of key-value pairs at any version.
	fastNodeCache  cache.Cache      // Cache for nodes in the fast index that represents only key-value pairs at the latest version.
}

func newNodeDB(db dbm.DB, cacheSize int, opts *Options, lg log.Logger) *nodeDB {
	if opts == nil {
		o := DefaultOptions()
		opts = &o
	}

	storeVersion, err := db.Get(metadataKeyFormat.Key([]byte(storageVersionKey)))

	if err != nil || storeVersion == nil {
		storeVersion = []byte(defaultStorageVersionValue)
	}

	return &nodeDB{
		logger:         lg,
		db:             db,
		batch:          db.NewBatch(),
		opts:           *opts,
		firstVersion:   0,
		latestVersion:  0, // initially invalid
		nodeCache:      cache.New(cacheSize),
		fastNodeCache:  cache.New(fastNodeCacheSize),
		versionReaders: make(map[int64]uint32, 8),
		storageVersion: string(storeVersion),
	}
}

// GetNode gets a node from memory or disk. If it is an inner node, it does not
// load its children.
func (ndb *nodeDB) GetNode(nk *NodeKey) (*Node, error) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	if nk == nil {
		return nil, ErrNodeMissingNodeKey
	}

	// Check the cache.
	if cachedNode := ndb.nodeCache.Get(nk.GetKey()); cachedNode != nil {
		ndb.opts.Stat.IncCacheHitCnt()
		return cachedNode.(*Node), nil
	}

	ndb.opts.Stat.IncCacheMissCnt()

	// Doesn't exist, load.
	buf, err := ndb.db.Get(ndb.nodeKey(nk))
	if err != nil {
		return nil, fmt.Errorf("can't get node %v: %v", nk, err)
	}
	if buf == nil {
		return nil, fmt.Errorf("Value missing for key %v corresponding to nodeKey %x", nk, ndb.nodeKey(nk))
	}

	node, err := MakeNode(nk, buf)
	if err != nil {
		return nil, fmt.Errorf("error reading Node. bytes: %x, error: %v", buf, err)
	}

	ndb.nodeCache.Add(node)

	return node, nil
}

func (ndb *nodeDB) GetFastNode(key []byte) (*fastnode.Node, error) {
	if !ndb.hasUpgradedToFastStorage() {
		return nil, errors.New("storage version is not fast")
	}

	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	if len(key) == 0 {
		return nil, fmt.Errorf("nodeDB.GetFastNode() requires key, len(key) equals 0")
	}

	if cachedFastNode := ndb.fastNodeCache.Get(key); cachedFastNode != nil {
		ndb.opts.Stat.IncFastCacheHitCnt()
		return cachedFastNode.(*fastnode.Node), nil
	}

	ndb.opts.Stat.IncFastCacheMissCnt()

	// Doesn't exist, load.
	buf, err := ndb.db.Get(ndb.fastNodeKey(key))
	if err != nil {
		return nil, fmt.Errorf("can't get FastNode %X: %w", key, err)
	}
	if buf == nil {
		return nil, nil
	}

	fastNode, err := fastnode.DeserializeNode(key, buf)
	if err != nil {
		return nil, fmt.Errorf("error reading FastNode. bytes: %x, error: %w", buf, err)
	}
	ndb.fastNodeCache.Add(fastNode)
	return fastNode, nil
}

// SaveNode saves a node to disk.
func (ndb *nodeDB) SaveNode(node *Node) error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	if node.nodeKey == nil {
		return ErrNodeMissingNodeKey
	}

	// Save node bytes to db.
	var buf bytes.Buffer
	buf.Grow(node.encodedSize())

	if err := node.writeBytes(&buf); err != nil {
		return err
	}

	if err := ndb.batch.Set(ndb.nodeKey(node.nodeKey), buf.Bytes()); err != nil {
		return err
	}

	// resetBatch only working on generate a genesis block
	if node.nodeKey.version <= genesisVersion {
		if err := ndb.resetBatch(); err != nil {
			return err
		}
	}

	ndb.logger.Debug("BATCH SAVE", "node", node)
	ndb.nodeCache.Add(node)
	return nil
}

// SaveFastNode saves a FastNode to disk and add to cache.
func (ndb *nodeDB) SaveFastNode(node *fastnode.Node) error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	return ndb.saveFastNodeUnlocked(node, true)
}

// SaveFastNodeNoCache saves a FastNode to disk without adding to cache.
func (ndb *nodeDB) SaveFastNodeNoCache(node *fastnode.Node) error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	return ndb.saveFastNodeUnlocked(node, false)
}

// setFastStorageVersionToBatch sets storage version to fast where the version is
// 1.1.0-<version of the current live state>. Returns error if storage version is incorrect or on
// db error, nil otherwise. Requires changes to be committed after to be persisted.
func (ndb *nodeDB) setFastStorageVersionToBatch() error {
	var newVersion string
	if ndb.storageVersion >= fastStorageVersionValue {
		// Storage version should be at index 0 and latest fast cache version at index 1
		versions := strings.Split(ndb.storageVersion, fastStorageVersionDelimiter)

		if len(versions) > 2 {
			return errInvalidFastStorageVersion
		}

		newVersion = versions[0]
	} else {
		newVersion = fastStorageVersionValue
	}

	latestVersion, err := ndb.getLatestVersion()
	if err != nil {
		return err
	}

	newVersion += fastStorageVersionDelimiter + strconv.Itoa(int(latestVersion))

	if err := ndb.batch.Set(metadataKeyFormat.Key([]byte(storageVersionKey)), []byte(newVersion)); err != nil {
		return err
	}
	ndb.storageVersion = newVersion
	return nil
}

func (ndb *nodeDB) getStorageVersion() string {
	return ndb.storageVersion
}

// Returns true if the upgrade to latest storage version has been performed, false otherwise.
func (ndb *nodeDB) hasUpgradedToFastStorage() bool {
	return ndb.getStorageVersion() >= fastStorageVersionValue
}

// Returns true if the upgrade to fast storage has occurred but it does not match the live state, false otherwise.
// When the live state is not matched, we must force reupgrade.
// We determine this by checking the version of the live state and the version of the live state when
// latest storage was updated on disk the last time.
func (ndb *nodeDB) shouldForceFastStorageUpgrade() (bool, error) {
	versions := strings.Split(ndb.storageVersion, fastStorageVersionDelimiter)

	if len(versions) == 2 {
		latestVersion, err := ndb.getLatestVersion()
		if err != nil {
			// TODO: should be true or false as default? (removed panic here)
			return false, err
		}
		if versions[1] != strconv.Itoa(int(latestVersion)) {
			return true, nil
		}
	}
	return false, nil
}

// saveFastNodeUnlocked saves a FastNode to disk.
func (ndb *nodeDB) saveFastNodeUnlocked(node *fastnode.Node, shouldAddToCache bool) error {
	if node.GetKey() == nil {
		return fmt.Errorf("cannot have FastNode with a nil value for key")
	}

	// Save node bytes to db.
	var buf bytes.Buffer
	buf.Grow(node.EncodedSize())

	if err := node.WriteBytes(&buf); err != nil {
		return fmt.Errorf("error while writing fastnode bytes. Err: %w", err)
	}

	if err := ndb.batch.Set(ndb.fastNodeKey(node.GetKey()), buf.Bytes()); err != nil {
		return fmt.Errorf("error while writing key/val to nodedb batch. Err: %w", err)
	}
	if shouldAddToCache {
		ndb.fastNodeCache.Add(node)
	}
	return nil
}

// Has checks if a hash exists in the database.
func (ndb *nodeDB) Has(nk *NodeKey) (bool, error) {
	key := ndb.nodeKey(nk)

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

// resetBatch reset the db batch, keep low memory used
func (ndb *nodeDB) resetBatch() error {
	size, err := ndb.batch.GetByteSize()
	if err != nil {
		// just don't do an optimization here. write with batch size 1.
		return ndb.writeBatch()
	}
	// write in ~64kb chunks. if less than 64kb, continue.
	if size < 64*1024 {
		return nil
	}

	return ndb.writeBatch()
}

func (ndb *nodeDB) writeBatch() error {
	var err error
	if ndb.opts.Sync {
		err = ndb.batch.WriteSync()
	} else {
		err = ndb.batch.Write()
	}
	if err != nil {
		return err
	}
	err = ndb.batch.Close()
	if err != nil {
		return err
	}

	ndb.batch = ndb.db.NewBatch()

	return nil
}

// deleteVersion deletes a tree version from disk.
// deletes orphans
func (ndb *nodeDB) deleteVersion(version int64) error {
	rootKey, err := ndb.GetRoot(version)
	if err != nil {
		return err
	}
	if rootKey == nil || rootKey.version < version {
		if err := ndb.batch.Delete(ndb.nodeKey(&NodeKey{version: version, nonce: 1})); err != nil {
			return err
		}
	}

	return ndb.traverseOrphans(version, func(orphan *Node) error {
		return ndb.batch.Delete(ndb.nodeKey(orphan.nodeKey))
	})
}

// DeleteVersionsFrom permanently deletes all tree versions from the given version upwards.
func (ndb *nodeDB) DeleteVersionsFrom(fromVersion int64) error {
	latest, err := ndb.getLatestVersion()
	if err != nil {
		return err
	}
	if latest < fromVersion {
		return nil
	}

	ndb.mtx.Lock()
	for v, r := range ndb.versionReaders {
		if v >= fromVersion && r != 0 {
			return fmt.Errorf("unable to delete version %v with %v active readers", v, r)
		}
	}
	ndb.mtx.Unlock()

	// Delete the nodes
	err = ndb.traverseRange(nodeKeyFormat.Key(fromVersion), nodeKeyFormat.Key(latest+1), func(k, v []byte) error {
		return ndb.batch.Delete(k)
	})

	if err != nil {
		return err
	}

	// NOTICE: we don't touch fast node indexes here, because it'll be rebuilt later because of version mismatch.

	ndb.resetLatestVersion(fromVersion - 1)

	return nil
}

// DeleteVersionsTo deletes the oldest versions up to the given version from disk.
func (ndb *nodeDB) DeleteVersionsTo(toVersion int64) error {
	first, err := ndb.getFirstVersion()
	if err != nil {
		return err
	}

	latest, err := ndb.getLatestVersion()
	if err != nil {
		return err
	}

	if toVersion < first || latest <= toVersion {
		return fmt.Errorf("the version should be in the range of [%d, %d)", first, latest)
	}

	for v, r := range ndb.versionReaders {
		if v >= first && v <= toVersion && r != 0 {
			return fmt.Errorf("unable to delete version %v with %v active readers", v, r)
		}
	}

	for version := first; version <= toVersion; version++ {
		if err := ndb.deleteVersion(version); err != nil {
			return err
		}
		ndb.resetFirstVersion(version + 1)
	}

	return nil
}

func (ndb *nodeDB) DeleteFastNode(key []byte) error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	if err := ndb.batch.Delete(ndb.fastNodeKey(key)); err != nil {
		return err
	}
	ndb.fastNodeCache.Remove(key)
	return nil
}

func (ndb *nodeDB) nodeKey(nk *NodeKey) []byte {
	return nodeKeyFormat.Key(nk.version, nk.nonce)
}

func (ndb *nodeDB) fastNodeKey(key []byte) []byte {
	return fastKeyFormat.KeyBytes(key)
}

func (ndb *nodeDB) getFirstVersion() (int64, error) {
	if ndb.firstVersion == 0 {
		latestVersion, err := ndb.getLatestVersion()
		if err != nil {
			return 0, err
		}
		firstVersion := int64(0)
		for firstVersion < latestVersion {
			version := (latestVersion + firstVersion) >> 1
			has, err := ndb.HasVersion(version)
			if err != nil {
				return 0, err
			}
			if has {
				latestVersion = version
			} else {
				firstVersion = version + 1
			}
		}
		ndb.firstVersion = latestVersion
	}
	return ndb.firstVersion, nil
}

func (ndb *nodeDB) resetFirstVersion(version int64) {
	ndb.firstVersion = version
}

func (ndb *nodeDB) getLatestVersion() (int64, error) {
	if ndb.latestVersion == 0 {
		itr, err := ndb.db.ReverseIterator(
			nodeKeyFormat.Key(int64(1)),
			nodeKeyFormat.Key(int64(math.MaxInt64)),
		)
		if err != nil {
			return 0, err
		}
		defer itr.Close()

		version := int64(-1)
		if itr.Valid() {
			k := itr.Key()
			nodeKeyFormat.Scan(k, &version)
			ndb.latestVersion = version
			return version, nil
		}

		if err := itr.Error(); err != nil {
			return 0, err
		}

		return 0, nil
	}
	return ndb.latestVersion, nil
}

func (ndb *nodeDB) resetLatestVersion(version int64) {
	ndb.latestVersion = version
}

// HasVersion checks if the given version exists.
func (ndb *nodeDB) HasVersion(version int64) (bool, error) {
	return ndb.db.Has(nodeKeyFormat.Key(version, []byte{1}))
}

// GetRoot gets the nodeKey of the root for the specific version.
func (ndb *nodeDB) GetRoot(version int64) (*NodeKey, error) {
	val, err := ndb.db.Get(nodeKeyFormat.Key(version, []byte{1}))
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, ErrVersionDoesNotExist
	}
	if len(val) == 0 { // empty root
		return nil, nil
	}
	if val[0] == nodeKeyFormat.Prefix()[0] { // point to the prev root
		var (
			version int64
			nonce   int32
		)
		nodeKeyFormat.Scan(val, &version, &nonce)
		return &NodeKey{version: version, nonce: nonce}, nil
	}

	return &NodeKey{version: version, nonce: 1}, nil
}

// SaveEmptyRoot saves the empty root.
func (ndb *nodeDB) SaveEmptyRoot(version int64) error {
	return ndb.batch.Set(nodeKeyFormat.Key(version, []byte{1}), []byte{})
}

// SaveRoot saves the root when no updates.
func (ndb *nodeDB) SaveRoot(version int64, prevRootKey *NodeKey) error {
	return ndb.batch.Set(nodeKeyFormat.Key(version, []byte{1}), ndb.nodeKey(prevRootKey))
}

// Traverse fast nodes and return error if any, nil otherwise
func (ndb *nodeDB) traverseFastNodes(fn func(k, v []byte) error) error {
	return ndb.traversePrefix(fastKeyFormat.Key(), fn)
}

// Traverse all keys and return error if any, nil otherwise

func (ndb *nodeDB) traverse(fn func(key, value []byte) error) error {
	return ndb.traverseRange(nil, nil, fn)
}

// Traverse all keys between a given range (excluding end) and return error if any, nil otherwise
func (ndb *nodeDB) traverseRange(start []byte, end []byte, fn func(k, v []byte) error) error {
	itr, err := ndb.db.Iterator(start, end)
	if err != nil {
		return err
	}
	defer itr.Close()

	for ; itr.Valid(); itr.Next() {
		if err := fn(itr.Key(), itr.Value()); err != nil {
			return err
		}
	}

	return itr.Error()
}

// Traverse all keys with a certain prefix. Return error if any, nil otherwise
func (ndb *nodeDB) traversePrefix(prefix []byte, fn func(k, v []byte) error) error {
	itr, err := dbm.IteratePrefix(ndb.db, prefix)
	if err != nil {
		return err
	}
	defer itr.Close()

	for ; itr.Valid(); itr.Next() {
		if err := fn(itr.Key(), itr.Value()); err != nil {
			return err
		}
	}

	return nil
}

// Get iterator for fast prefix and error, if any
func (ndb *nodeDB) getFastIterator(start, end []byte, ascending bool) (dbm.Iterator, error) {
	var startFormatted, endFormatted []byte

	if start != nil {
		startFormatted = fastKeyFormat.KeyBytes(start)
	} else {
		startFormatted = fastKeyFormat.Key()
	}

	if end != nil {
		endFormatted = fastKeyFormat.KeyBytes(end)
	} else {
		endFormatted = fastKeyFormat.Key()
		endFormatted[0]++
	}

	if ascending {
		return ndb.db.Iterator(startFormatted, endFormatted)
	}

	return ndb.db.ReverseIterator(startFormatted, endFormatted)
}

// Write to disk.
func (ndb *nodeDB) Commit() error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	var err error
	if ndb.opts.Sync {
		err = ndb.batch.WriteSync()
	} else {
		err = ndb.batch.Write()
	}
	if err != nil {
		return fmt.Errorf("failed to write batch, %w", err)
	}

	ndb.batch.Close()
	ndb.batch = ndb.db.NewBatch()

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

// traverseOrphans traverses orphans which removed by the updates of the version (n+1).
func (ndb *nodeDB) traverseOrphans(version int64, fn func(*Node) error) error {
	curKey, err := ndb.GetRoot(version + 1)
	if err != nil {
		return err
	}

	curIter, err := NewNodeIterator(curKey, ndb)
	if err != nil {
		return err
	}

	prevKey, err := ndb.GetRoot(version)
	if err != nil {
		return err
	}
	prevIter, err := NewNodeIterator(prevKey, ndb)
	if err != nil {
		return err
	}

	var orgNode *Node
	for prevIter.Valid() {
		for orgNode == nil && curIter.Valid() {
			node := curIter.GetNode()
			if node.nodeKey.version <= version {
				curIter.Next(true)
				orgNode = node
			} else {
				curIter.Next(false)
			}
		}
		pNode := prevIter.GetNode()

		if orgNode != nil && bytes.Equal(pNode.hash, orgNode.hash) {
			prevIter.Next(true)
			orgNode = nil
		} else {
			err = fn(pNode)
			if err != nil {
				return err
			}
			prevIter.Next(false)
		}
	}

	return nil
}

// Utility and test functions

func (ndb *nodeDB) leafNodes() ([]*Node, error) {
	leaves := []*Node{}

	err := ndb.traverseNodes(func(node *Node) error {
		if node.isLeaf() {
			leaves = append(leaves, node)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return leaves, nil
}

func (ndb *nodeDB) nodes() ([]*Node, error) {
	nodes := []*Node{}

	err := ndb.traverseNodes(func(node *Node) error {
		nodes = append(nodes, node)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return nodes, nil
}

func (ndb *nodeDB) orphans() ([][]byte, error) {
	orphans := [][]byte{}

	for version := ndb.firstVersion; version < ndb.latestVersion; version++ {
		err := ndb.traverseOrphans(version, func(orphan *Node) error {
			orphans = append(orphans, orphan.hash)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	return orphans, nil
}

// Not efficient.
// NOTE: DB cannot implement Size() because
// mutations are not always synchronous.
//

func (ndb *nodeDB) size() int {
	size := 0
	err := ndb.traverse(func(k, v []byte) error {
		size++
		return nil
	})
	if err != nil {
		return -1
	}
	return size
}

func isReferenceToRoot(bz []byte) bool {
	if bz[0] == nodeKeyFormat.Prefix()[0] {
		if len(bz) == 13 {
			return true
		}
	}
	return false
}

func (ndb *nodeDB) traverseNodes(fn func(node *Node) error) error {
	nodes := []*Node{}

	if err := ndb.traversePrefix(nodeKeyFormat.Key(), func(key, value []byte) error {
		if isReferenceToRoot(value) {
			return nil
		}
		var (
			version int64
			nonce   int32
		)
		nodeKeyFormat.Scan(key, &version, &nonce)
		node, err := MakeNode(&NodeKey{
			version: version,
			nonce:   nonce,
		}, value)
		if err != nil {
			return err
		}
		nodes = append(nodes, node)
		return nil
	}); err != nil {
		return err
	}

	sort.Slice(nodes, func(i, j int) bool {
		return bytes.Compare(nodes[i].key, nodes[j].key) < 0
	})

	for _, n := range nodes {
		if err := fn(n); err != nil {
			return err
		}
	}
	return nil
}

// traverseStateChanges iterate the range of versions, compare each version to it's predecessor to extract the state changes of it.
// endVersion is exclusive, set to `math.MaxInt64` to cover the latest version.
func (ndb *nodeDB) traverseStateChanges(startVersion, endVersion int64, fn func(version int64, changeSet *ChangeSet) error) error {
	firstVersion, err := ndb.getFirstVersion()
	if err != nil {
		return err
	}
	if startVersion < firstVersion {
		startVersion = firstVersion
	}
	latestVersion, err := ndb.getLatestVersion()
	if err != nil {
		return err
	}
	if endVersion > latestVersion {
		endVersion = latestVersion
	}

	prevVersion := startVersion - 1
	prevRoot, err := ndb.GetRoot(prevVersion)
	if err != nil && err != ErrVersionDoesNotExist {
		return err
	}

	for version := startVersion; version <= endVersion; version++ {
		root, err := ndb.GetRoot(version)
		if err != nil {
			return err
		}

		var changeSet ChangeSet
		receiveKVPair := func(pair *KVPair) error {
			changeSet.Pairs = append(changeSet.Pairs, pair)
			return nil
		}

		if err := ndb.extractStateChanges(prevVersion, prevRoot, root, receiveKVPair); err != nil {
			return err
		}

		if err := fn(version, &changeSet); err != nil {
			return err
		}
		prevVersion = version
		prevRoot = root
	}

	return nil
}

func (ndb *nodeDB) String() (string, error) {
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()

	index := 0

	err := ndb.traversePrefix(nodeKeyFormat.Key(), func(key, value []byte) error {
		fmt.Fprintf(buf, "%s: %x\n", key, value)
		return nil
	})
	if err != nil {
		return "", err
	}

	buf.WriteByte('\n')

	err = ndb.traverseNodes(func(node *Node) error {
		switch {
		case node == nil:
			fmt.Fprintf(buf, "%s: <nil>\n", nodeKeyFormat.Prefix())
		case node.value == nil && node.subtreeHeight > 0:
			fmt.Fprintf(buf, "%s: %s   %-16s h=%d nodeKey=%v\n",
				nodeKeyFormat.Prefix(), node.key, "", node.subtreeHeight, node.nodeKey)
		default:
			fmt.Fprintf(buf, "%s: %s = %-16s h=%d nodeKey=%v\n",
				nodeKeyFormat.Prefix(), node.key, node.value, node.subtreeHeight, node.nodeKey)
		}
		index++
		return nil
	})

	if err != nil {
		return "", err
	}

	return "-" + "\n" + buf.String() + "-", nil
}

var ErrNodeMissingNodeKey = fmt.Errorf("node does not have a nodeKey")
