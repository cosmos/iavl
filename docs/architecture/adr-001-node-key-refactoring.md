# ADR ADR-001: Node Key Refactoring

## Changelog

- 2022-10-31: First draft

## Status

Proposed

## Context

The original key format of IAVL nodes is a hash of the node. It does not take advantage of data locality on disk. Nodes are stored in random locations on the disk due to the random hash value, so it needs to scan the disk to find the corresponding node which can be very inefficient.

The `orphans` are used to manage node removal in the current design and allow the deletion of removed nodes for the specific version from the disk through the `DeleteVersion` API. It needs to track every time when updating the tree and also requires extra storage to store `orphans`. But there are only 2 use cases for `DeleteVersion`:

1. Rollback of the tree to a previous version
2. Remove unnecessary old nodes

## Decision

- Use the version and the sequenced integer ID as a node key like `bigendian(version) | bigendian(nonce)` format. 
- Remove the `version` field from node body writes.
- Remove the `leftHash` and `rightHash` fields, and instead store `hash` field in the node body.
- Separate the `orphans` from the tree CRUD operations, and refactor the orphan store like `bigendian(to_version) | bigendian(from_version) -> nonce`.
- Remove the `root` store and prefix identifier which is used to identify the root, node, and orphan.

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
	nodeKey       *NodeKey   // new field, the key in the storage
	leftNodeKey   *NodeKey   // new field, need to store in the storage
	rightNodeKey  *NodeKey   // new field, need to store in the storage
	leftNode      *Node
	rightNode     *Node
    size          int64
	subtreeHeight int8
}
```

New tree structure

```go
type MutableTree struct {
	*ImmutableTree                                    
	lastSaved                *ImmutableTree
	nonce					 int32
	versions                 map[int64]bool           
	allRootLoaded            bool                     
	unsavedFastNodeAdditions map[string]*fastnode.Node
	unsavedFastNodeRemovals  map[string]interface{}   
	ndb                      *nodeDB
	skipFastStorageUpgrade   bool 

	mtx sync.Mutex
}
```

We will assign the nonce in order when saving the current version in `SaveVersion`. It will reduce unnecessary checks in CRUD operations of the tree and keep sorted the order of insertion in the LSM tree.

### Migration

We can migrate nodes one by one by iterating the version.

- Iterate the version in order, and get the root node for the specific version.
- Iterate the tree based on the root and pick only nodes the node version is the same as the given version.
- Store them using the new node key format.

### Pruning

We introduce a new way to struct `orphans` in the `SaveVersion`, not in the `Set` or `Remove`.

- Get the previous root from the `lastSaved`.
- Iterate the tree until `leftNode` or `rightNode` is not `nil` based on the previous root.

The above node group would be removed in the current version because having children means it is updated in the current CRUD operations.

### Rollback

When we want to rollback to the specific version `n`

- Iterate the version from `n+1`.
- Traverse key-value through `traversePrefix` with `prefix=bigendian(version)`.
- Remove data (it will include `orphans` and `nodes` data).

## Consequences

### Positive

Using the version and the sequenced integer ID, we take advantage of data locality in the LSM tree. Since we commit the sorted data, it can reduce compactions and makes it easy to find the key. Also, it can reduce the key and node size in the storage.

```
# node body

add `hash`:											+32 byte
add `leftNodeKey`, `rightNodeKey`:	max (8+4)*2=	+24 byte
remove `leftHash`, `rightHash`:						-64 byte
remove `version`: 					max 			-8	byte
------------------------------------------------------------
									total save		16	byte

# node key

remove `hash`:				-32 byte
add `version|nonce`:		+12 byte
------------------------------------
				total save 	20 	byte
```

Separating orphans also provides performance improvements including memory and storage saving.

### Negative

The `Update` operation will require extra DB access because we need to take children to calculate the hash of updated nodes.
It doesn't require more access in other cases including `Set`, `Remove`, and `Proof`.

## References

- https://github.com/cosmos/iavl/issues/548
- https://github.com/cosmos/iavl/issues/137
- https://github.com/cosmos/iavl/issues/571
