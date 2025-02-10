# iavl/v2

IAVL v2 is performance minded rewrite of IAVL v1.  Benchmarks show a 10-20x improvement in
throughput depending on the operation.  The primary changes are:

- Checkpoints: periodic writes of dirty branch nodes to disk.
- Leaf changelog: leaf nodes are flushed to disk at every version. 
- Replay: revert the tree to a previous version by replaying the leaf changelog.
- Sharding: shards are created on pruning events.
- BTree on disk: SQLite (a mature BTree implementation) is used for storage.
- Cache: the AVL tree is cached in memory and (non-dirty) nodes evicted by configurable policy.

## Concepts

### Checkpoints

A checkpoint writes all dirty branch nodes currently in memory since the last checkpoint to
disk. Checkpoints are distinct from shards.  One shard may contain multiple checkpoints.  A checkpoint occurs
at a configurable interval or when the dirty branch nodes exceed a threshold.

### Leaf Changelog

The leaf changelog is a list of leaf nodes that have been written since the last checkpoint.  Inserts and 
updates are in one table, deletes in another.  They are ordered by a sequence number per version to allow for
deterministic replay.  The also makes it possible to evict leafs from the tree and rely on SQLite's
page cache and memory map to manage efficient access for leaves.

### Replay

Replay is the process of reverting the tree to a previous version.  Given a version v, the tree is loaded at 
the check version m less than or equal to v.  The leaf changelog is replayed from m to v.  The tree is now at
version v.

This is useful for rolling back, or querying and proving the state of the tree at a previous version.

### Sharding

A shard contains all the changes to a tree from version m to version n.  It may contain multiple checkpoints.

### BTree (SQLite)

Why SQLite? A B+Tree is a very efficient on disk data structure.  The ideal implementation of IAVL on disk
would be to lay out nodes in subtrees chunks in the same format as the in-memory AVL tree.  A B+Tree is a
as close an approximation to this as possible.

## Pruning

Parameters:

- invalidated ratio: the ratio of invalidated nodes to total nodes in a shard that triggers a
  pruning event.  The default is 1.5.  Roughly correleates to disk size of a complete tree, where (2 * ratio) is the size of the pre preuned, tree on disk.  A ratio of 1.5 means that 3x the initial size should be provisioned.
- minumum keep versions: the minimum number of versions to keep.  This is a safety feature to
  prevent pruning to a version that is too recent.  The default is 100.

Pruning events only occur on checkpoint boundaries.  The prune version is the most recent check
point less than or equal to the requested prune version.

On prune the latest shard is locked (readonly) and a new shard is created.  The new shard is now
the hot shard and subsequent SaveVersion calls write leafs and branches to it.

Deletes happen by writing a new shard without orphans, updating the shard connection, then
dropping the old one.
