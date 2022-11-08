# ADR ADR-001: Node Key Refactoring

## Changelog

- 2022-10-31: First draft

## Status

Proposed

## Context

The original key format of IAVL nodes is a hash of the node. It does not take advantage of data locality on disk. Nodes are stored in random locations on disk due to the random hash value, so it needs to scan the disk to find the corresponding node which can be very inefficient.

The `orphans` are used to manage node removal in the current design and allow deletion of removed nodes for the specific version from the disk through the `DeleteVersion` API. It needs to track every time when updating the tree and also requires extra storage to store `orphans`. But there are only 2 use cases for `DeleteVersion`:

1. Rollback of the tree to a previous version
2. Remove unnecessary old nodes

## Decision

- Use the version and the sequenced integer ID as a node key like `bigendian(version) | bigendian(nonce)` format. 
- Remove the `version` field from node body writes.
- Remove the `leftHash` and `rightHash` fields, and instead store `hash` field.
- Remove the `orphans` from the tree.

New node structure

```go
type NodeKey struct {
    version int64
    nonce int32
}

type Node struct {
	key           []byte
	value         []byte
	hash          []byte    // keep it in the storage instead of leftHash and rightHash
	nodeKey       NodeKey   // new field, the key in the storage
	leftNodeKey   NodeKey   // new field, need to store in the storage
	rightNodeKey  NodeKey   // new field, need to store in the storage
	leftNode      *Node
	rightNode     *Node
    size          int64
	subtreeHeight int8
	persisted     bool
}
```

New tree structure

```go
type MutableTree struct {
	*ImmutableTree                                    
	lastSaved                *ImmutableTree
	versions                 map[int64]bool           
	allRootLoaded            bool                     
	unsavedFastNodeAdditions map[string]*fastnode.Node
	unsavedFastNodeRemovals  map[string]interface{}   
	ndb                      *nodeDB
	skipFastStorageUpgrade   bool 

	mtx sync.Mutex
}
```

We will restruct the nonce when save the current version. It will reduce unnecessary checks in CRUD operations of the tree and keep sorted the order of insertion in the LSM tree.

### Migration

We can migrate nodes one by one by iterating the version.

- Iterate the version in order, and get the root node for the specific version.
- Iterate the tree and assign the `nodeKey` to nodes which the node version equals. 

We will implement the `Import` functionality for the original version.

### Pruning

We introduce a new way to prune old versions.

For example, when a user wants to prune the previous 500 versions every 1000 blocks
- We iterate the tree based on the `n+501`th root and pick only nodes which the version is in `[(n+1), (n+500)]`.
- We can delete missing nodes (which don't appear in the above traversing) instantly or re-assign the nonce in order for those nodes. Re-assign should be done after stopping the node but it can lead to improving the data locality.

## Consequences

### Positive

Using the version and the sequenced integer ID, we take advantage of data locality in the LSM tree. Since we commit the sorted data, it can reduce compactions and makes easy to find the key. Also, it can reduce the key and node size in the storage.

Removing orphans also provides performance improvements including memory and storage saving. Also, it makes it easy to rollback the tree. Because we can remove all nodes for which the `nodeKey` is greater than the specified version.

### Negative

It requires extra storage to store the node because it should keep `leftNodeKey` and `rightNodeKey` to iterate the tree. Instead, we can delete`leftHash` and `rightHash` fields in the node and reduce the key size.

Remvoing old nodes for the specific version requires some extra iterations due to removing orphans. 

## References

- https://github.com/cosmos/iavl/issues/548
- https://github.com/cosmos/iavl/issues/137
- https://github.com/cosmos/iavl/issues/571
