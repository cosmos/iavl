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

	dbm "github.com/cosmos/cosmos-db"

	"github.com/cosmos/iavl/cache"
	"github.com/cosmos/iavl/fastnode"
	"github.com/cosmos/iavl/internal/encoding"
	"github.com/cosmos/iavl/internal/logger"
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
	// possible with the other keys, and makes them easier to traverse. They are indexed by the version and the node local nonce.
	nodeKeyFormat = keyformat.NewKeyFormat('n', int64Size, int32Size) // n<version><nonce>

	// Orphans are keyed in the database by their expected lifetime.
	// The first number represents the *last* version at which the orphan needs
	// to exist, while the second number represents the *earliest* version at
	// which it is expected to exist - which starts out by being the version
	// of the node being orphaned.
	// To clarify:
	// When I write to key {X} with value V and old value O, we orphan O with <last-version>=time of write
	// and <first-version> = version O was created at.
	orphanKeyFormat = keyformat.NewKeyFormat('n', int64Size, int32Size, int64Size, int32Size) // n<last-version><0><first-version><nonce>

	// Key Format for making reads and iterates go through a data-locality preserving db.
	// The value at an entry will list what version it was written to.
	// Then to query values, you first query state via this fast method.
	// If its present, then check the tree version. If tree version >= result_version,
	// return result_version. Else, go through old (slow) IAVL get method that walks through tree.
	fastKeyFormat = keyformat.NewKeyFormat('f', 0) // f<keystring>

	// Key Format for storing metadata about the chain such as the vesion number.
	// The value at an entry will be in a variable format and up to the caller to
	// decide how to parse.
	metadataKeyFormat = keyformat.NewKeyFormat('m', 0) // m<keystring>

	// Keep alive versions.
	versionKeyFormat = keyformat.NewKeyFormat('v', int64Size) // v<version>
)

var errInvalidFastStorageVersion = fmt.Sprintf("Fast storage version must be in the format <storage version>%s<latest fast cache version>", fastStorageVersionDelimiter)

type nodeDB struct {
	mtx            sync.Mutex       // Read/write lock.
	db             dbm.DB           // Persistent node storage.
	batch          dbm.Batch        // Batched writing buffer.
	opts           Options          // Options to customize for pruning/writing
	versionReaders map[int64]uint32 // Number of active version readers
	storageVersion string           // Storage version
	latestVersion  int64            // Latest version of nodeDB.
	nodeCache      cache.Cache      // Cache for nodes in the regular tree that consists of key-value pairs at any version.
	fastNodeCache  cache.Cache      // Cache for nodes in the fast index that represents only key-value pairs at the latest version.
}

func newNodeDB(db dbm.DB, cacheSize int, opts *Options) *nodeDB {
	if opts == nil {
		o := DefaultOptions()
		opts = &o
	}

	storeVersion, err := db.Get(metadataKeyFormat.Key(unsafeToBz(storageVersionKey)))

	if err != nil || storeVersion == nil {
		storeVersion = []byte(defaultStorageVersionValue)
	}

	return &nodeDB{
		db:             db,
		batch:          db.NewBatch(),
		opts:           *opts,
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

	logger.Debug("BATCH SAVE %+v\n", node)
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
			return errors.New(errInvalidFastStorageVersion)
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

// DeleteVersion deletes a tree version from disk.
// calls deleteOrphans(version), deleteRoot(version, checkLatestVersion)
func (ndb *nodeDB) DeleteVersion(version int64, checkLatestVersion bool) error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	if ndb.versionReaders[version] > 0 {
		return fmt.Errorf("unable to delete version %v, it has %v active readers", version, ndb.versionReaders[version])
	}

	err := ndb.deleteOrphans(version)
	if err != nil {
		return err
	}

	err = ndb.deleteVersion(version, checkLatestVersion)
	if err != nil {
		return err
	}
	return err
}

// DeleteVersionsFrom permanently deletes all tree versions from the given version upwards.
func (ndb *nodeDB) DeleteVersionsFrom(version int64) error {
	latest, err := ndb.getLatestVersion()
	if err != nil {
		return err
	}
	if latest < version {
		return nil
	}

	for v, r := range ndb.versionReaders {
		if v >= version && r != 0 {
			return fmt.Errorf("unable to delete version %v with %v active readers", v, r)
		}
	}

	// Delete the nodes and orphans
	err = ndb.traverseRange(nodeKeyFormat.Key(version, int32(0)), nodeKeyFormat.Key(latest, int32(math.MaxInt32)), func(k, v []byte) error {
		if err = ndb.batch.Delete(k); err != nil {
			return err
		}
		return nil
	})
	// Delete orphans for version-1
	err = ndb.traverseRange(orphanKeyFormat.Key(version-1, int32(0)), orphanKeyFormat.Key(version-1, int32(1)), func(k, v []byte) error {
		if err = ndb.batch.Delete(k); err != nil {
			return err
		}
		return nil
	})
	// Delete the version entries
	err = ndb.traverseRange(versionKeyFormat.Key(version), versionKeyFormat.Key(int64(math.MaxInt64)), func(k, v []byte) error {
		if err = ndb.batch.Delete(k); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}

	// Delete fast node entries
	err = ndb.traverseFastNodes(func(keyWithPrefix, v []byte) error {
		key := keyWithPrefix[1:]
		fastNode, err := fastnode.DeserializeNode(key, v)
		if err != nil {
			return err
		}

		if version <= fastNode.GetVersionLastUpdatedAt() {
			if err = ndb.batch.Delete(keyWithPrefix); err != nil {
				return err
			}
			ndb.fastNodeCache.Remove(key)
		}
		return nil
	})

	if err != nil {
		return err
	}

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

	latest, err := ndb.getLatestVersion()
	if err != nil {
		return err
	}
	if latest < toVersion {
		return fmt.Errorf("cannot delete latest saved version (%d)", latest)
	}

	predecessor, err := ndb.getPreviousVersion(fromVersion)
	if err != nil {
		return err
	}

	for v, r := range ndb.versionReaders {
		if v < toVersion && v >= fromVersion && r != 0 {
			return fmt.Errorf("unable to delete version %v with %v active readers", v, r)
		}
	}

	// If the predecessor is earlier than the beginning of the lifetime, we can delete the orphan.
	// Otherwise, we shorten its lifetime, by moving its endpoint to the predecessor version.
	for version := fromVersion; version < toVersion; version++ {
		err := ndb.traverseOrphansVersion(version, func(key, _ []byte) error {
			var from, to int64
			var dummy, nonce int32
			orphanKeyFormat.Scan(key, &to, &dummy, &from, &nonce)
			nk := &NodeKey{
				version: from,
				nonce:   nonce,
			}
			if err := ndb.batch.Delete(key); err != nil {
				return err
			}
			if from > predecessor {
				if err := ndb.batch.Delete(ndb.nodeKey(nk)); err != nil {
					return err
				}
				ndb.nodeCache.Remove(nk.GetKey())
			} else {
				if err := ndb.saveOrphan(nk, predecessor); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	// Delete the version entries
	err = ndb.traverseRange(versionKeyFormat.Key(fromVersion), versionKeyFormat.Key(toVersion), func(k, v []byte) error {
		if err := ndb.batch.Delete(k); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
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

// Saves orphaned nodes to disk under a special prefix.
// version: the new version being saved.
// orphans: the orphan nodes created since version-1
func (ndb *nodeDB) SaveOrphans(version int64, orphans []*NodeKey) error {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()

	toVersion, err := ndb.getPreviousVersion(version)
	if err != nil {
		return err
	}

	for _, nk := range orphans {
		logger.Debug("SAVEORPHAN %d-%v\n", toVersion, nk)
		err := ndb.saveOrphan(nk, toVersion)
		if err != nil {
			return err
		}
	}
	return nil
}

// Saves a single orphan to disk.
func (ndb *nodeDB) saveOrphan(nk *NodeKey, toVersion int64) error {
	if nk.version > toVersion {
		return fmt.Errorf("orphan expires before it comes alive.  %d > %d", nk.version, toVersion)
	}
	key := ndb.orphanKey(toVersion, nk)
	if err := ndb.batch.Set(key, []byte{}); err != nil {
		return err
	}
	return nil
}

// deleteOrphans deletes orphaned nodes from disk, and the associated orphan
// entries.
func (ndb *nodeDB) deleteOrphans(version int64) error {
	// Will be zero if there is no previous version.
	predecessor, err := ndb.getPreviousVersion(version)
	if err != nil {
		return err
	}

	// Traverse orphans with a lifetime ending at the version specified.
	// TODO optimize.
	return ndb.traverseOrphansVersion(version, func(key, _ []byte) error {
		var fromVersion, toVersion int64
		var dummy, nonce int32

		// See comment on `orphanKeyFmt`. Note that here, `version` and
		// `toVersion` are always equal.
		orphanKeyFormat.Scan(key, &toVersion, &dummy, &fromVersion, &nonce)
		nk := &NodeKey{
			version: fromVersion,
			nonce:   nonce,
		}
		// Delete orphan key and reverse-lookup key.
		if err := ndb.batch.Delete(key); err != nil {
			return err
		}

		// If there is no predecessor, or the predecessor is earlier than the
		// beginning of the lifetime (ie: negative lifetime), or the lifetime
		// spans a single version and that version is the one being deleted, we
		// can delete the orphan.  Otherwise, we shorten its lifetime, by
		// moving its endpoint to the previous version.
		if predecessor < fromVersion {
			logger.Debug("DELETE toVersion:%d predecessor:%d node key:%v\n", toVersion, predecessor, nk)
			if err := ndb.batch.Delete(ndb.nodeKey(nk)); err != nil {
				return err
			}
			ndb.nodeCache.Remove(nk.GetKey())
		} else {
			logger.Debug("MOVE toVersion:%d predecessor:%d node key:%v\n", toVersion, predecessor, nk)
			err := ndb.saveOrphan(nk, predecessor)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (ndb *nodeDB) nodeKey(nk *NodeKey) []byte {
	return nodeKeyFormat.Key(nk.version, nk.nonce)
}

func (ndb *nodeDB) fastNodeKey(key []byte) []byte {
	return fastKeyFormat.KeyBytes(key)
}

func (ndb *nodeDB) orphanKey(toVersion int64, orphan *NodeKey) []byte {
	return orphanKeyFormat.Key(toVersion, int32(0), orphan.version, orphan.nonce)
}

func (ndb *nodeDB) versionKey(version int64) []byte {
	return versionKeyFormat.Key(version)
}

func (ndb *nodeDB) getLatestVersion() (int64, error) {
	if ndb.latestVersion == 0 {
		var err error
		ndb.latestVersion, err = ndb.getPreviousVersion(1<<63 - 1)
		if err != nil {
			return 0, err
		}
	}
	return ndb.latestVersion, nil
}

func (ndb *nodeDB) updateLatestVersion(version int64) {
	if ndb.latestVersion < version {
		ndb.latestVersion = version
	}
}

func (ndb *nodeDB) resetLatestVersion(version int64) {
	ndb.latestVersion = version
}

func (ndb *nodeDB) getPreviousVersion(version int64) (int64, error) {
	itr, err := ndb.db.ReverseIterator(
		versionKeyFormat.Key(1),
		versionKeyFormat.Key(version),
	)
	if err != nil {
		return 0, err
	}
	defer itr.Close()

	pversion := int64(-1)
	if itr.Valid() {
		k := itr.Key()
		versionKeyFormat.Scan(k, &pversion)
		return pversion, nil
	}

	if err := itr.Error(); err != nil {
		return 0, err
	}

	return 0, nil
}

// deleteVersion deletes the version entry from disk, but not the node it points to.
func (ndb *nodeDB) deleteVersion(version int64, checkLatestVersion bool) error {
	latestVersion, err := ndb.getLatestVersion()
	if err != nil {
		return err
	}

	if checkLatestVersion && version == latestVersion {
		return errors.New("tried to delete latest version")
	}
	if err := ndb.batch.Delete(ndb.versionKey(version)); err != nil {
		return err
	}
	return nil
}

func (ndb *nodeDB) HasVersion(version int64) (bool, error) {
	return ndb.db.Has(ndb.versionKey(version))
}

func (ndb *nodeDB) getVersions() (versions []int64, err error) {
	versions = make([]int64, 0)
	err = ndb.traversePrefix(versionKeyFormat.Key(), func(k, _ []byte) error {
		var version int64
		versionKeyFormat.Scan(k, &version)
		versions = append(versions, version)
		return nil
	})
	return versions, err
}

// GetRoot get the nodeKey of the root for the specific version.
func (ndb *nodeDB) GetRoot(version int64) (*NodeKey, error) {
	value, err := ndb.db.Get(versionKeyFormat.Key(version))
	if err != nil {
		return nil, err
	}
	rootVersion, _, err := encoding.DecodeVarint(value)
	return &NodeKey{version: rootVersion, nonce: 1}, err
}

// SaveRoot creates an entry on disk for the given root, so that it can be
// loaded later.
func (ndb *nodeDB) SaveRoot(version, rootVersion int64) error {
	buf := new(bytes.Buffer)
	if err := encoding.EncodeVarint(buf, rootVersion); err != nil {
		return err
	}
	if err := ndb.batch.Set(ndb.versionKey(version), buf.Bytes()); err != nil {
		return err
	}
	ndb.updateLatestVersion(version)

	return nil
}

// Traverse orphans and return error if any, nil otherwise
func (ndb *nodeDB) traverseOrphans(fn func(keyWithPrefix, v []byte) error) error {
	ndb.resetLatestVersion(0)
	latest, err := ndb.getLatestVersion()
	if err != nil {
		return err
	}

	for version := int64(1); version <= latest; version++ {
		if err := ndb.traversePrefix(orphanKeyFormat.Key(version, int32(0)), fn); err != nil {
			return err
		}
	}
	return nil
}

// Traverse fast nodes and return error if any, nil otherwise
func (ndb *nodeDB) traverseFastNodes(fn func(k, v []byte) error) error {
	return ndb.traversePrefix(fastKeyFormat.Key(), fn)
}

// Traverse orphans ending at a certain version. return error if any, nil otherwise
func (ndb *nodeDB) traverseOrphansVersion(version int64, fn func(k, v []byte) error) error {
	return ndb.traversePrefix(orphanKeyFormat.Key(version, int32(0)), fn)
}

// Traverse all keys and return error if any, nil otherwise
// nolint: unused
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

	if err := itr.Error(); err != nil {
		return err
	}

	return nil
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

// Utility and test functions

// nolint: unused
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

// nolint: unused
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

// nolint: unused
func (ndb *nodeDB) orphans() ([][]byte, error) {
	orphans := [][]byte{}

	err := ndb.traverseOrphans(func(k, v []byte) error {
		orphans = append(orphans, k)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return orphans, nil
}

// Not efficient.
// NOTE: DB cannot implement Size() because
// mutations are not always synchronous.
//
//nolint:unused
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

func (ndb *nodeDB) traverseNodes(fn func(node *Node) error) error {
	ndb.resetLatestVersion(0)
	latest, err := ndb.getLatestVersion()
	if err != nil {
		return err
	}

	nodes := []*Node{}
	for version := int64(1); version <= latest; version++ {
		if err := ndb.traverseRange(nodeKeyFormat.Key(version, int32(1)), nodeKeyFormat.Key(version, int32(math.MaxInt32)), func(key, value []byte) error {
			var nk NodeKey
			nodeKeyFormat.Scan(key, &nk.version, &nk.nonce)
			node, err := MakeNode(&nk, value)
			if err != nil {
				return err
			}
			nodes = append(nodes, node)
			return nil
		}); err != nil {
			return err
		}
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

func (ndb *nodeDB) String() (string, error) {
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()

	index := 0

	err := ndb.traversePrefix(versionKeyFormat.Key(), func(key, value []byte) error {
		fmt.Fprintf(buf, "%s: %x\n", key, value)
		return nil
	})
	if err != nil {
		return "", err
	}

	buf.WriteByte('\n')

	err = ndb.traverseOrphans(func(key, value []byte) error {
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

var (
	ErrNodeMissingNodeKey   = fmt.Errorf("node does not have a nodeKey")
	ErrNodeAlreadyPersisted = fmt.Errorf("shouldn't be calling save on an already persisted node")
	ErrRootMissingNodeKey   = fmt.Errorf("root node key must not be zero")
)
