# impl

special handling for node with dirty bit? since they will never be evicted perhaps don't need to check 
for a fault?

track bytes in pool instead of node count. this is a more realistic metric for memory usage.

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

### clean up

aesthetics: migrate node to active record pattern with a pool handle so that `.left(tree)` -> `.left()`

### open

more graceful handling of overflow. as-is overflow may cause many nodes in the tree to have nil `leftNode` 
and `rightNode` which results in a fault and db lookup on next iteration.  this is a performance hit, and 
the result of using go managed memory for overflow nodes.  ideally the overflow nodes would be managed by 
the node pool, but then we need to shrink the pool after it has grown which is tricky.