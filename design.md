# Design doom thoughts

## checkpoint

Also called snapshot.

A checkpoint occurs when the pool working set is in an overflow state (dirty node size > soft ceiling),
or a specified number of blocks have elapsed since the last checkpoint.  The pool is briefly locked so
that a `lock` bit can be set on each node pending checkpoint write.  `node.mutate` should refuse to
mutate a node with `lock = true`, instead returning a new node from the pool. Checkpoints are running
async.  Atomic bool performance should be tested, if used `lock = false` may be set as nodes are written,
otherwise a second global lock after the checkpoint is done can update `lock = false`.

`overflow` nodes are always included in the checkpoint.  Since these are outside of the pool (managed by
go GC) they must be collected in `tree.SaveVersion`.  An alternate design could have these accumulate in
a `pool.overflow []*Node` field.

Generally this mimics the behavior of a double buffer so long as there is space in the pool.

`lock` and `dirty` are beginning to look like `refCount int`.

## modes of operation

IAVL (SC) persistence operating modes:

- `latest` - only the latest version is persisted at checkpoints. orphans are collected in memory and
  removed at checkpoints where `orphan.version <= lastCheckpointVersion`. this has the effect of slowly
  collapsing and shrinking past checkpoints. it uses the least amount of disk space. highest throughput,
  probably useful for validators or query nodes which don't need to maintain a full history in SC. similar
  to `pruning = nothing` but an order of magnitude faster.

- `snapshot` - each checkpoint materializes a full snapshot to disk, that is, all nodes where
  `node.version > lastCheckpointVersion`. checkpoint interval will always be honored, but the soft memory
  ceiling may cause additional checkpoints to occur. a pruning process should run periodically to collapse
  checkpoints into the desired cadence. a good balance between disk space, throughput, and replayability
  when used with SS. proofs can be generated for any _v_ where there is a snapshot version _v' <= v_ by
  replaying the changelog in SS.

- `head` - the last _n_ states are persisted to disk. this is a the legacy pruning behavior of IAVL. a
  pruning process manages removing old states from disk. proofs just fetch tree nodes directly from disk by
  loading a root node and iterating a tree with one disk read per node.  same behavior as `pruning=default`

- `full` - all nodes are persisted to disk at every version. this is the legacy archive node behavior
  `pruning = nothing` of IAVL.

## cache full

Question: when the cache is full, how do we proceed? large working sets are a problem.

the working set, `dirty = true`, should perhaps be unbounded with a soft ceiling defined by count or bytes.
when the ceiling is reached we always flush the working set to disk and set `dirty = false` on the next
`Commit`.   using one slice for the entire cache we would need to (1) grow the cache, (2) empty out the
working set by copying to a new slice, or (3) never place the working set in cache in the first place.

exploring option (3) a bit.  we never evict from the working set, so this could make CLOCK more efficient.
working set size should be tracked so that the soft ceiling can be enforced.  flushing the working set
means 1) writing every node to disk, 2) clear the node, 3) remove all overflowed nodes (past the soft
ceiling) if there are any.  this means we no longer need a `dirty` bit at all.

growing the cache (1) may also be possible.  probably keep a count of dirty and clean nodes for statistics.
a naive strategy: when `len(free) < len(cache) / 2` grow the cache by 2x.  when dirty node size exceeds
the soft ceiling we flush.  question: how to shrink the cache?  when `len(free) > len(cache) / 2` shrink
the cache by 2x.  I think the pool must be locked so that the entire pool may enumerated and frameIds
reassigned in order to reclaim space.

pressure from clean nodes should not cause the cache to grow though, only pressure from dirty nodes.
since this is the case option (3) makes more sense.

question: when we shrink the cache how do we choose which nodes to evict? run the CLOCK? perhaps shrinking
should be very infrequent, but supported. so should growing.

a final option (4) is to spill the working set to disk when the soft ceiling is reached.  this means that
`nodeKey` must be generated up front (not in `SaveVersion`) so that there is a key to save the node under.
to save IO `SaveVersion` must be able to handle nodes which are already persisted, and `addOrphan` must be
able to handle nodes which are already persisted.  this means decoupling the node pool flush cadence from
the tree version cadence completely.  
(4) does not seem viable unless we create a separate volatile storage area for the working set. none
of the nodes persisted here contain proper hashes, so they cannot be used in the tree.  the entire set is
removed on `Commit()`, and nodes are potentially loaded during `SaveVersion()`.

perhaps overflow working set into temporary memory is the best solution.  these nodes will not make it
back into the primary cache, possibly resulting into some inefficient churn, but we can accept this
performance hit since hopefully the memory settings making working set overflow infrequent. a soft ceiling
to trigger flush should prevent overflow from occurring too often.
