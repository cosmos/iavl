# iavl-v2 (v6) design

## State Commitment

This is a prune-free state commitment (SC) design similar to MemIAVL featuring periodic checkpoints
(snapshots) to disk.  Only the most recent SC tree is kept in memory. Instead of mmap a page cache is used,
giving us more control over memory management.  Instead of a custom snapshot format persisted nodes are
written to an LSM tree.

SC maintains one large page cache.  The eviction policy is CLOCK with two bits on each node, `use` and
`dirty`. `use` indicates that the node was recently used and `dirty` indicates the node is part of the
current working set.  CLOCK will unset a `use` bit when seen but only the checkpoint process will unset a
`dirty` bit.

The cache maintains working sets, `hot` and `cold`. `hot` accumulates working nodes as they created and
mutated in the IAVL tree. Once the page cache reaches a memory threshold *or* a certain amount of time
(blocks) has elapsed since the last flush, the checkpoint process starts and `hot` is flushed to disk.
This involves swapping `hot` and `cold`, enumerating the nodes in the set, writing to disk and clearing
the `dirty` bit.  `cold` is quickly emptied while `hot` slowly fills up.  This double buffer strategy
prevents the need to lock the page cache or pause the IAVL tree while checkpointing.

Instead of IAVL traversal fetching nodes from a slice or other external data structure as a buffer pool,
the pointers `node.leftNode` and `node.rightNode` just fetch directly from the managed memory.  A node pool
is maintained as a slice of pre-allocated `Node` structs.  This is effectively the page cache and has the
additional benefit of relieving GC pressure. Therefore, when fetching left or right nodes the following
tasks must be performed: 1) fetch the node from the cache by reference, 2) compare node keys, then 3a) if
equal set the `use` bit, or 3b) if unequal (page fault) fetch the node from disk then evict and replace a
node in cache.

### Reference counting and orphan cleanup

Periodic snapshot cleanup (collapsing and deleting unreferenced tree nodes) is required to prevent
exploding database size. This can be performed by a background process using the IAVL tree diff algorithm
introduced in [iavl#646](https://github.com/cosmos/iavl/pull/646) to identify and delete orphans
(unreferenced nodes).

Another strategy is to accumulate a list of orphaned nodes during tree operations where `orphan.version <
lastCheckpointVersion`.  This list is then used to delete orphans from disk during the next checkpoint.
The overhead of both approaches should be measured.

#### Branching

If branching is to be supported as a feature of SC (instead of store v1 in `CacheKV`) then reference
counting must be implemented in the page cache and an additional `refCount int` is required on each page.
Mutating a node in the page cache working set where `refCount > 1` is disallowed since this means the
node is shared by multiple branches.  Perhaps `refCount int` replaces `dirty bool`.

## State Storage

An ideal (maybe naive) state storage (SS) design flushes leaves (application key-value pairs) are
directly on every commit. SS therefore behaves as a write-ahead log (WAL).  If SS writes are fast enough
a proper WAL is not needed.

If however an append-only WAL has better write performance a similar strategy to state commitment (SC) can
be used to increase throughput.  Namely, an in-memory working set to service reads which is periodically
flushed at checkpoint intervals to disk and the WAL truncated.  This presumes that writing in batches to SS
is more performant than every block, which may the case for certain SS backends, or due to the decreased
disk read IO resulting from the in-memory working set.

Should leaf nodes values only be stored in SS and never in SC.
