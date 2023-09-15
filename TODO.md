# TODO

## clean up

aesthetics: migrate node to active record pattern with a pool handle so that `.left(tree)` -> `.left()`

`tree_test.go` should be in an `iavl_test` package, this means an audit of exported fields and funcs in 
`iavl`.

the `levelDb` package should not be in `iavl` package and a dependency on `cosmos-db` should be not be formed.
it is included for now for ease of testing. forming on `iavl_test` is probably OK.

the `encoding` package should be interrogated. a version of `writeHashBytes` was implemented without it. 
need benchmarks to see if it is worth it, or point them if they already exist.

## 

node.Block in changesets not incrementing after benchmark gen refactor

