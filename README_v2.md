# iavl/v2

IAVL v2 is performance minded rewrite of IAVL v1.  Benchmarks show a 10-20x improvement in
throughput depending on the operation.  The primary changes are:

- Checkpoints: periodic writes of dirty nodes to disk.
- Sharding: shards are created on pruning events.
- BTree on disk: SQLite (a mature BTree implementation) is used for storage.

## Checkpoints

A checkpoint writes all dirty branch nodes currently in memory since the last checkpoint to
disk. Checkpoints are distinct from shards.  One shard may contain multiple checkpoints.  

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
