# ADR ADR-001: Node Key Refactoring

## Changelog

- 2022-10-31: First draft

## Status

Proposed

## Context

The original node key of IAVL is a hash of the node and it does not take advantage of data locality on disk. The nodes are stored in a random location of the disk due to the random hash value, so it needs to do a random search of the disk to find the node.

The `orphans` are used to manage the removed nodes in the current version and allow to deletion of the removed nodes for the specific version from the disk through the `DeleteVersion`. It needs to track every time when updating the tree and also requires extra storage to store `orphans`, but there are not many use cases of `DeleteVersion`. There are two use cases, the first one is the rollback of the tree and the second one is to remove the unnecessary old nodes.

## Decision

- Use the sequenced integer ID as a node key like `bigendian(nodeKey)` format.
- Remove the `leftHash` and `rightHash` fields, and instead store `hash` field.
- Remove the `version` field from the node structure.
- Remove the `orphans` from the tree.

New node structure

```go
type Node struct {
	key           []byte
	value         []byte
	hash          []byte    // keep this field
	leftHash      []byte    // will remove
	rightHash     []byte    // will remove
	nodeKey       int64     // new field, use as a node key
	leftNodeKey   int64     // new field, need to store
	rightNodeKey  int64     // new field, need to store
	version       int64     // will remove
	size          int64
	leftNode      *Node
	rightNode     *Node
	subtreeHeight int8
	persisted     bool
}
```

New tree structure

```go
type MutableTree struct {
	*ImmutableTree                                    
	lastSaved                *ImmutableTree
	nonce                    int64                    // new field to track the current ID
	orphans                  map[int64]int64          // will remove
	versions                 map[int64]bool           
	allRootLoaded            bool                     
	unsavedFastNodeAdditions map[string]*fastnode.Node
	unsavedFastNodeRemovals  map[string]interface{}   
	ndb                      *nodeDB
	skipFastStorageUpgrade   bool 

	mtx sync.Mutex
}
```

## Consequences

### Positive

Using the sequenced integer ID, we take advantage of data locality in the bTree and it leads to performance improvements. Also it can reduce the node size in the storage.

Removing orphans also provides performance improvements including memory and storage saving. Also, it makes it easy to rollback the tree. Because we will keep the sequenced segment IDs for the specific version, and we can remove all nodes for which the `nodeKey` is greater than the specified integer value.

### Negative

It requires extra storage to store the node because it should keep `leftNodeKey` and `rightNodeKey` to iterate the tree. Instead, we can delete the `version`, `leftHash` and `rightHash` fields in the node and reduce the key size.

It can't delete the old nodes for the specific version due to removing orphans. But it makes `rollback` easier and it makes it possible to remove old nodes through `import` and `export` functionalities. The `export` will restruct the tree to make node IDs to a sequenced segment like (1 ... node_sieze).

## References

- https://github.com/cosmos/iavl/issues/548
- https://github.com/cosmos/iavl/issues/137
- https://github.com/cosmos/iavl/issues/571
