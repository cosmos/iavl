# impl

special handling for node with dirty bit? since they will never be evicted perhaps don't need to check 
for a fault?

track bytes in pool instead of node count. this is a more realistic metric for memory usage.

## clean up

aesthetics: migrate node to active record pattern with a pool handle so that `.left(tree)` -> `.left()`

`tree_test.go` should be in an `iavl_test` package, this means an audit of exported fields and funcs in 
`iavl`.

the `levelDb` package should not be in `iavl` package and a dependency on `cosmos-db` should be not be formed.
it is included for now for ease of testing. forming on `iavl_test` is probably OK.

the `encoding` package should be interrogated. a version of `writeHashBytes` was implemented without it. 
need benchmarks to see if it is worth it, or point them if they already exist.

## open

more graceful handling of overflow. as-is overflow may cause many nodes in the tree to have nil `leftNode`
and `rightNode` which results in a fault and db lookup on next iteration.  this is a performance hit, and
the result of using go managed memory for overflow nodes.  ideally the overflow nodes would be managed by
the node pool, but then we need to shrink the pool after it has grown which is tricky.

## overflow

overflow nodes into a read-only ring buffer.  
