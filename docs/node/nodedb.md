# NodeDB

### Structure

The nodeDB is responsible for persisting nodes, orphans, and roots correctly in temporary and persistent storage. It also handles all pruning logic given a passed in pruning strategy. The default behavior is to prune nothing and persist every version to disk.

The nodeDB maintains two seperate databases and batches. The `recentDB` is for temporary storage (typically `memDB`) and the `snapshotDB` is for persistent storage (typically `levelDB`).

The nodeDB takes in a PruningStrategy to determine how many recent versions to save and at what frequency to persist to disk. Recent versions are saved in `memDB`, while snapshot versions are persisted to disk.

### Saving Versions

When an IAVL tree is saved, the nodeDB first checks the version against the PruningStrategy. If the version is a snapshot version, then the IAVL tree gets saved both to the `recentDB` and `snapshotDB`. If not, the tree is saved only to the `recentDB`.

The nodeDB saves the roothash of the IAVL tree under the key: `r|<version>`.

It marshals and saves any new node that has been created under: `n|<hash>`. For more details on how the node gets marshaled, see [node documentation](./node.md). Any old node that is still part of the latest IAVL tree will not get rewritten. Instead its parent will simply have a hash pointer with which the nodeDB can retrieve the old node if necessary.

Any old nodes that were part of the previous version IAVL but are no longer part of this one have been saved in an orphan map `orphan.hash => orphan.version`. This map will get passed into the nodeDB's `SaveVersion` function. The map maps from the orphan's hash to the version that it was added to the IAVL tree. The nodeDB iterates through this map and stores each marshalled orphan node under the key: `o|toVersion|fromVersion`. Since the toVersion is always the previous version (if we are saving version `v`, toVersion of all new orphans is `v-1`), we can save the orphans by iterating over the map and saving: `o|(latestVersion-1)|orphan.fromVersion => orphan.hash`.

(For more details on key formats see the [keyformat docs](./key_format.md))

##### Persisting Orphans to Disk

The SaveOrphans algorithm works slightly differently when a custom PruningStrategy is used such that `strategy.KeepEvery > 1`. 

If a snapshot version exists between an orphans `fromVersion` and `toVersion` inclusive, then that orphan needs to be persisted to disk since a snapshot version refers to it.

However, it will get persisted to disk with a toVersion equal to the highest snapshot version that contains it rather than the generally highest predecessor.

```golang
// Logic for determining if orphan should be persisted to disk
flushToDisk := false
if ndb.opts.KeepEvery != 0 {
    // if snapshot version in between fromVersion and toVersion INCLUSIVE, then flush to disk.
    flushToDisk = fromVersion/ndb.opts.KeepEvery != toVersion/ndb.opts.KeepEvery || ndb.isSnapshotVersion(fromVersion)
}
```

```golang
// Logic for saving to disk with toVersion that is highest snapshotVersion <= original toVersion
// save to disk with toVersion equal to snapshotVersion closest to original toVersion
snapVersion := toVersion - (toVersion % ndb.opts.KeepEvery)
key := ndb.orphanKey(fromVersion, snapVersion, hash)
ndb.snapshotBatch.Set(key, hash)
```

### Deleting Versions

When a version `v` is deleted, the roothash corresponding to version `v` is deleted from nodeDB. All orphans whose `toVersion = v`, will get the `toVersion` pushed back to the highest predecessor of `v` that still exists in nodeDB. If the `toVersion <= fromVersion` then this implies that there does not exist a version of the IAVL tree in the nodeDB that still contains this node. Thus, it can be safely deleted and uncached.

When deleting a version, the caller can specify if the deletion is supposed to be from `memoryOnly` or completely deleted. If `memOnly` argument is true, then deletion only happens on `recentDB`. Else, IAVL version gets deleted both from `recentDB` and `snapshotDB`.

##### Deleting Orphans

The deleteOrphans algorithm is shown below:

```golang
// deleteOrphans deletes orphaned nodes from disk, and the associated orphan
// entries.
func (ndb *nodeDB) deleteOrphans(version int64) {
	// Will be zero if there is no previous version.
	predecessor := ndb.getPreviousVersion(version)

	// Traverse orphans with a lifetime ending at the version specified.
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
			ndb.batch.Delete(ndb.nodeKey(hash))
			ndb.uncacheNode(hash)
		} else {
			ndb.saveOrphan(hash, fromVersion, predecessor)
		}
	})
}
```

### Pruning Versions

If the nodeDB is passed in a PruningStrategy with `strategy.keepRecent != 0`, it will maintain the specified number of recent versions in the memDB.
When `PruneRecentVersions`, the IAVL version `v - strategy.keepRecent` will be deleted from `recentDB` (Calls `DeleteVersion(v - strategy.KeepRecent, memOnly=true)`). This ensures that at any given point, there are only `strategy.keepRecent` versions in `recentDB`.

Note this is not called immediately in `nodeDB`, the caller (in most cases `MutableTree`) is responsible for calling `PruneRecentVersions` after each save to ensure that `recentDB` is always holding at most `keepRecent` versions.

`PruneRecentVersions` will return the version numbers that no longer exist in the nodeDB. If the version that got pruned is a snapshot version, `PruneRecentVersions` returns `nil` since the version will still exist in the `snapshotDB`. Else, `PruneRecentVersions` will return a list containing the pruned version number.
