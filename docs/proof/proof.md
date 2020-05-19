# Proofs

What sets IAVL apart from most other key/value stores is the ability to return
[Merkle proofs](https://en.wikipedia.org/wiki/Merkle_tree) along with values. These proofs can
be used to verify that a returned value is, in fact, the value contained within a given IAVL tree.
This verification is done by comparing the proof's root hash with the tree's root hash.

Somewhat simplified, an IAVL tree is a variant of a
[binary search tree](https://en.wikipedia.org/wiki/Binary_search_tree) where inner nodes contain 
keys used for binary search, and leaf nodes contain the actual key/value pairs. Consider the 
following example, containing five key/value pairs (e.g. key `a` with value `1`):

```
            d
          /   \
        c       e
      /   \    /  \
    b     c=3 d=4 e=5
  /   \
a=1   b=2
```

In reality, IAVL nodes contain more data than shown here - for details please refer to the
[node documentation](../node/node.md). However, this simplified version is sufficient for a
high-level understanding.

A cryptographically secure hash is generated for each node in the tree by hashing the key, value
(if any), version, and height of the node as well as the hashes of each direct child (if any).
This implies that the hash of any given node is also a hash of all descendants of the node. In
turn, this implies that the hash of the root node is a hash of all nodes (and therefore all
data) in the tree.

If we fetch the value `a=1` from the tree and want to verify that this is the correct value, we
need the following information:

```
                 d
               /   \
             c     hash=d6f56d
           /   \
         b     hash=ec6088
       /   \
a,hash(1)  hash=92fd030
```

Note that we take the hash of the value `a=1` instead of simply using the value; both would work,
but the value can be arbitrarily large while the hash has a constant size.

With this data, we are able to compute the hashes for all nodes up to and including the root,
and can compare this root hash with the root hash of the IAVL tree - if they match, we can be
reasonably certain that the value is correct. This data is therefore considered a _proof_ for the
value. Notice how we don't need to include any data from e.g. the `e`-branch of the tree at all,
only the hash - as the tree grows in size, these savings become very significant.

However, this still introduces quite a bit of overhead. Since we usually want to fetch several
values from the tree and verify them, it is often useful to fetch a _range proof_, which contains
a proof for a contiguous set of key/value leaf nodes. For example, the following proof can
verify both `a=1`, `b=2`, and `c=3`:

```
                 d
               /   \
             c     hash=d6f56d
           /   \
         b     c,hash(3)
       /   \
a,hash(1)  b,hash(2)
```

Range proofs can also be used to prove the _absence_ of a key. This is done by producing a range
proof of the keys directly before and after the absent key - if the proof root matches the tree
root, and the proof does not include the leaf node for the key, then the key cannot be in the tree.

## API Overview

The following is a general overview of the API - for details, see the
[API reference](https://pkg.go.dev/github.com/tendermint/iavl).

As an example, we will be using the same IAVL tree as outlined in the introduction:

```
            d
          /   \
        c       e
      /   \    /  \
    b     c=3 d=4 e=5
  /   \
a=1   b=2
```

This tree can be generated as follows:

```go
package main

import (
	"fmt"
	"log"

	"github.com/tendermint/iavl"
	db "github.com/tendermint/tm-db"
)

func main() {
	tree, err := iavl.NewMutableTree(db.NewMemDB(), 0)
	if err != nil {
		log.Fatal(err)
	}

	tree.Set([]byte("e"), []byte{5})
	tree.Set([]byte("d"), []byte{4})
	tree.Set([]byte("c"), []byte{3})
	tree.Set([]byte("b"), []byte{2})
	tree.Set([]byte("a"), []byte{1})

	rootHash, version, err := tree.SaveVersion()
	if err != nil {
		log.Fatal(err)
	}
	_, _ = rootHash, version // ignore variables

    // Output tree structure, including all node hashes (prefixed with 'n')
	fmt.Printf("%x\n", tree.Hash())
}
```

### The Tree Root Hash

Proofs are verified against the root hash of an IAVL tree. This root hash is retrived via
`MutableTree.Hash()` or `ImmutableTree.Hash()`, returning a `[]byte` hash. It is also returned by 
`MutableTree.SaveVersion()`, as shown above.

```go
fmt.Printf("%x\n", tree.Hash())
// Outputs: dd21329c026b0141e76096b5df395395ae3fc3293bd46706b97c034218fe2468
```

### Generating Proofs

The following methods are used to generate proofs, all of which are of type `RangeProof`:

* `ImmutableTree.GetWithProof(key []byte)`: fetches the key's value (if it exists) along with a
  proof of existence or proof of absence.

* `ImmutableTree.GetRangeWithProof(start, end []byte, limit int)`: fetches the keys, values, and 
  proofs for the given key range.

* `MutableTree.GetVersionedWithProof(key []byte, version int64)`: like `GetWithProof()`, but for a
  specific version of the tree.

* `MutableTree.GetVersionedRangeWithProof(key []byte, version int64)`: like `GetRangeWithProof()`, 
  but for a specific version of the tree.

### Verifying Proofs

To verify that a `RangeProof` is valid for a given IAVL tree (i.e. that the proof root hash matches
the tree root hash), run `RangeProof.Verify()` with the tree's root hash:

```go
// Generate a proof for a=1
value, proof, err := tree.GetWithProof([]byte("a"))
if err != nil {
    log.Fatal(err)
}

err = proof.Verify(tree.Hash())
if err != nil {
    log.Fatalf("Invalid proof: %v", err)
}
```

The proof must _always_ be verified against the root hash with `Verify()` - other operations
will assume the proof is valid. The verification can also be done manually with
`RangeProof.ComputeRootHash()`:

```go
if !bytes.Equal(proof.ComputeRootHash(), tree.Hash()) {
    log.Fatal("Proof hash mismatch")
}
```

To verify that a key has a given value according to the proof, use `RangeProof.VerifyItem()`
on a proof generated for this key (or key range):

```go
// The proof was generated for the item a=1, so this is successful
err = proof.VerifyItem([]byte("a"), []byte{1})
if err != nil {
    log.Printf("Failed to prove a=1: %v", err)
}

// If we instead claim that a=2, the proof will error
err = proof.VerifyItem([]byte("a"), []byte{2})
if err != nil {
    log.Printf("Failed to prove a=2: %v", err)
}

// Also, verifying b=2 errors even though correct, since the proof is for a=1
err = proof.VerifyItem([]byte("b"), []byte{2})
if err != nil {
    log.Printf("Failed to prove b=2: %v", err)
}
```

If we generate a proof for a range of keys, we can use this both to prove any of the keys in the
range, as well as the absence of any keys that would be within it:

```go
// Note that the end key is not inclusive, so c is not in the proof
keys, values, proof, err := tree.GetRangeWithProof([]byte("a"), []byte("c"))
if err != nil {
    log.Fatal(err)
}

err = proof.Verify(tree.Hash())
if err != nil {
    log.Fatal(err)
}

err = proof.VerifyItem([]byte("a"), []byte{1})
if err != nil {
    log.Printf("Failed to prove a=1 with range: %v", err)
}

err = proof.VerifyItem([]byte("c"), []byte{3})
if err != nil {
    log.Printf("Failed to prove c=3 with range: %v", err)
}

err = proof.VerifyAbsence([]byte("ab"))
if err != nil {
    log.Printf("Failed to verify absence of ab: %v", err)
}
```

### Proof Structure

The overall proof structure was described in the introduction, here we will have a look at the
actual data structure. Knowledge of this is not necessary to use proofs.

A `RangeProof` contains the following data, as well as JSON tags for easy serialization:

```go
type RangeProof struct {
	LeftPath   PathToLeaf      `json:"left_path"`
	InnerNodes []PathToLeaf    `json:"inner_nodes"`
	Leaves     []ProofLeafNode `json:"leaves"`
}
```

`LeftPath` 

To fully understand this, we will first understand what `PathToLeaf` and `ProofInnerNode` are.

### ProofInnerNode

`ProofInnerNode` is a struct that simply holds information about a node in the IAVL tree.
It records the following information:

```golang
type ProofInnerNode struct {
	Height  int8   `json:"height"`
	Size    int64  `json:"size"`
	Version int64  `json:"version"`
	Left    []byte `json:"left"`
	Right   []byte `json:"right"`
}
```

This holds a subset of the `Node` struct. For information about what each field means, please
refer to the [`Node` documentation](../node/node.md).

### PathToLeaf

`PathToLeaf` is a list of `ProofInnerNode`, where the list is ordered from furthest to closet to
the root node. For example, for the following path in the tree:

    Root
       \
        A
         \
          B
           \
            C
           
PathToLeaf will be storing nodes in the order of [C, B, A]

### ProofLeafNode

`ProofLeafNode` is similiar to `ProofInnerNode` where it holds some information about the leaf
node without the extra path information.

```golang
type ProofLeafNode struct {
	Key       cmn.HexBytes `json:"key"`
	ValueHash cmn.HexBytes `json:"value"`
	Version   int64        `json:"version"`
}
```

### RangeProof

Now that we understand what each field type in a `RangeProof` means, we can understand what a 
`RangeProof` is:

```golang
type RangeProof struct {
	LeftPath   PathToLeaf      `json:"left_path"`
	InnerNodes []PathToLeaf    `json:"inner_nodes"`
	Leaves     []ProofLeafNode `json:"leaves"`
}
```

Essentially a `RangeProof` represents a proof that a range of keys between either exists or is
absent from the IAVL tree. This is important as `RangeProof` needs to support both verifying a
single key (`GetWithProof`) and list of keys (`GetRangeWithProof`).

`LeftPath` stores the path in the IAVL tree from root to the leaf node with the first key
(keyStart), with a list of `ProofInnerNode`s. `InnerNodes` stores all the remaining paths (list
of `PathToLeafs`) in the tree to get to all the remaining `Leaves`. `Leaves` keeps all the leaf
nodes that is part of the key range, which each leaf node represents a particular key in the
IAVL tree.

### Verifying RangeProof

With a `RangeProof`, we can both verify that it's a valid proof by traversing the list of
`PathToLeaf` and verifying each hash that is stored with the rest of the nodes, and in the end
compare if this root hash matches what's being stored in the IAVL tree.

## Proof Operations

Now that we understand the fields that are part of the `Range` proof, we can start to understand
which operations are supported.

The three operations a range proof provides are 1) Verify 2) VerifyItem 3) VerifyAbsence.

### Verify

One can verify a root hash and compares with the root hash that is computed from the `RangeProof`
is equal. As long as the tree content and the hashing mechanism remains the same, the root hash
over a merkle tree should always be the same. This is used to verify that a `RangeProof` is
valid, since the root hash computed from the proof and tree should be identical.
  
## VerifyItem

Once a `RangeProof` is verified it can be used to prove that a key/value within the range exists
in this `RangeProof`. Since all the keys as part of the `RangeProof` are stored in `Leaves`,
verifying a particular key and value is simply verifying one of the leaf node's key matches the
key and its value's hash matches the value hash stored.

## VerifyAbsence

Similarly to VerifiyItem, once a `RangeProof` is verified it can be used to prove that a
key/value within the range doesn't exist. Proving that a key doesn't exist from a `RangeProof`
is accomplished by verifying that the key doesn't exist in the Leaves.