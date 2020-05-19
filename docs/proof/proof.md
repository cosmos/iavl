# Proof

Part of the purpose of an IAVL tree is to provide the ability to return proofs along with values,
which can be later used to verify if the provided proof is indeed valid from the IAVL merkle tree.

Users can call `GetWithProof()`, `GetVersionedWithProof()`, `GetRangeWithProof()`, or 
`GetRangeVersionedWithProof()` for a given key, which will return the corresponding value along 
with a `RangeProof`. A later section will explain in more detail what a `RangeProof` consists of, 
and how IAVL produces and verifies it.

IAVL provides APIs to verify, using a range proof, whether 1) the range proof is valid
(`Verify`) 2) an item key/value exists (`VerifyItem`), and 3) an item key/value doesn't exist
(`VerifyAbsence`).

## RangeProof

`RangeProof` is returned when a key is requested with a proof. A `RangeProof` is defined as the
following:

```golang
type RangeProof struct {
	LeftPath   PathToLeaf      `json:"left_path"`
	InnerNodes []PathToLeaf    `json:"inner_nodes"`
	Leaves     []ProofLeafNode `json:"leaves"`
}
```

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