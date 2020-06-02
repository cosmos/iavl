## 

<div align="center">
  <h1> IAVL+ Tree </h1>
</div>


<div align="center">
  <a href="https://github.com/tendermint/iavl/releases/latest">
    <img alt="Version" src="https://img.shields.io/github/tag/tendermint/iavl.svg" />
  </a>
  <a href="https://github.com/tendermint/iavl/blob/master/LICENSE">
    <img alt="License: Apache-2.0" src="https://img.shields.io/github/license/tendermint/iavl.svg" />
  </a>
  <a href="https://pkg.go.dev/github.com/tendermint/iavl?tab=doc">
    <img alt="go doc" src="https://godoc.org/github.com/tendermint/iavl?status.svg" />
  </a>
  <a href="https://goreportcard.com/report/github.com/tendermint/iavl">
    <img alt="go report card" src="https://goreportcard.com/badge/github.com/tendermint/iavl" />
  </a>
  <a href="https://codecov.io/gh/tendermint/iavl">
    <img alt="Code Coverage" src="https://codecov.io/gh/tendermint/iavl/branch/master/graph/badge.svg" />
  </a>
</div>
<div align="center">
  <a href="https://discord.gg/AzefAFd">
    <img alt="Discord" src="https://img.shields.io/discord/669268347736686612.svg" />
  </a>
    <img alt="Test" src="https://github.com/tendermint/iavl/workflows/Test/badge.svg?branch=master" />
    <img alt="Lint Satus" src="https://github.com/tendermint/iavl/workflows/Lint/badge.svg?branch=master" />
</div>

**Note: Requires Go 1.13+**

A versioned, snapshottable (immutable) AVL+ tree for persistent data.

The purpose of this data structure is to provide persistent storage for key-value pairs (say to store account balances) such that a deterministic merkle root hash can be computed. The tree is balanced using a variant of the [AVL algorithm](http://en.wikipedia.org/wiki/AVL_tree) so all operations are O(log(n)).

Nodes of this tree are immutable and indexed by their hash. Thus any node serves as an immutable snapshot which lets us stage uncommitted transactions from the mempool cheaply, and we can instantly roll back to the last committed state to process transactions of a newly committed block (which may not be the same set of transactions as those from the mempool).

In an AVL tree, the heights of the two child subtrees of any node differ by at most one. Whenever this condition is violated upon an update, the tree is rebalanced by creating O(log(n)) new nodes that point to unmodified nodes of the old tree. In the original AVL algorithm, inner nodes can also hold key-value pairs. The AVL+ algorithm (note the plus) modifies the AVL algorithm to keep all values on leaf nodes, while only using branch-nodes to store keys. This simplifies the algorithm while keeping the merkle hash trail short.

In Ethereum, the analog is [Patricia tries](http://en.wikipedia.org/wiki/Radix_tree). There are tradeoffs. Keys do not need to be hashed prior to insertion in IAVL+ trees, so this provides faster iteration in the key space which may benefit some applications. The logic is simpler to implement, requiring only two types of nodes -- inner nodes and leaf nodes. On the other hand, while IAVL+ trees provide a deterministic merkle root hash, it depends on the order of transactions. In practice this shouldn't be a problem, since you can efficiently encode the tree structure when serializing the tree contents.
