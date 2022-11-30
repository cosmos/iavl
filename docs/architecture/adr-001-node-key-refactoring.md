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

- Use the version and the sequenced integer ID as a node key like `bigendian(version) | byte array(path)` format. Here the `path` is a binary expression of the path from the root to the current node. 
	```
	`10101` : (right, left, right, left, right) -> [0x15]
	```
- Store only the child node key for the below version in node body writes. Because it is possible to get the child path for the same version.
	```go
	func (node *Node) getLeftNode() (*Node, error) {
		if node.leftNode != nil {
			return node.leftNode
		}
		if node.leftNodeKey != nil {
			return getNode(node.leftNodeKey) // get the node from the storage
		}
		return getNode(&NodeKey{
			version: node.nodeKey.version,
			path: node.nodeKey.path + '0',  // it will be more complicated in the real implementation
		})
	}
	```
- Remove the `version` field from node body writes.
- Remove the `leftHash` and `rightHash` fields, and instead store `hash` field in the node body.
- Remove the `orphans` completely from both tree and storage.

New node structure

```go
type NodeKey struct {
    version int64
    path 	[]byte
}

type Node struct {
	key           []byte
	value         []byte
	hash          []byte     // keep it in the storage instead of leftHash and rightHash
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
	versions                 map[int64]bool           
	allRootLoaded            bool                     
	unsavedFastNodeAdditions map[string]*fastnode.Node
	unsavedFastNodeRemovals  map[string]interface{}   
	ndb                      *nodeDB
	skipFastStorageUpgrade   bool 

	mtx sync.Mutex
}
```

We will assign the `nodeKey` when saving the current version in `SaveVersion`. It will reduce unnecessary checks in CRUD operations of the tree and keep sorted the order of insertion in the LSM tree.

### Migration

We can migrate nodes one by one by iterating the version.

- Iterate the version in order, and get the root node for the specific version.
- Iterate the tree based on the root and pick only nodes the node version is the same as the given version.
- Store them using the new node key format.

### Pruning

We assume keeping only the range versions of `fromVersion` to `toVersion`. Refer to [this issue](https://github.com/cosmos/cosmos-sdk/issues/12989).

When we want to prune all versions up to the specific version `n`

- Iterate the tree based on the root of `n+1`th version.
- Iterate the node until visiting the node the version is below `fromVersion` and don't visit further deeply.
- Apply `DeletePath` for all visited nodes the version is below `n+1`.

```go
func DeletePath(nk *NodeKey) error {
	DeleteNode(node)
	if nk.path is not root {
		DeletePath(&NodeKey{
			version: nk.version,
			path: parent(nk.path), // it looks like removing the last binary
		})
	}
}
```

### Rollback

When we want to rollback to the specific version `n`

- Iterate the version from `n+1`.
- Traverse key-value through `traversePrefix` with `prefix=bigendian(version)`.
- Remove all iterated nodes.

## Consequences

### Positive

Using the version and the path, we take advantage of data locality in the LSM tree. Since we commit the sorted data, it can reduce compactions and makes it easy to find the key. Also, it can reduce the key and node size in the storage.

```
# node body

add `hash`:											+32 byte
add `leftNodeKey`, `rightNodeKey`:	max 8 + 8	=	+16 byte
remove `leftHash`, `rightHash`:						-64 byte
remove `version`: 					max 			-8	byte
------------------------------------------------------------
									total save		24	byte

# node key

remove `hash`:				-32 byte
add `version|path`:			+16 byte
------------------------------------
				total save 	16 	byte
```

Removing orphans also provides performance improvements including memory and storage saving.

### Negative

The `Update` operation will require extra DB access because we need to take children to calculate the hash of updated nodes.
It doesn't require more access in other cases including `Set`, `Remove`, and `Proof`.

It is impossible to remove the individual version. The new design requires more restrict pruning strategies.

## References

- https://github.com/cosmos/iavl/issues/548
- https://github.com/cosmos/iavl/issues/137
- https://github.com/cosmos/iavl/issues/571
- https://github.com/cosmos/cosmos-sdk/issues/12989
