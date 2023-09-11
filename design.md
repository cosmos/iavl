# Design doom thoughts

### cache full

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
