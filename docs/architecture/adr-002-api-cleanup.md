# ADR ADR-002: API Cleanup to Support Store V2

## Changelog

* 2023-06-06: First draft

## Status

DRAFT

## Abstract

This ADR proposes a cleanup of the API to support [Store V2](https://github.com/cosmos/cosmos-sdk/blob/main/docs/architecture/adr-065-store-v2.md).

There are some proposals for the speedup of the `Commit` by the async writes. See the [Discussion](https://github.com/cosmos/cosmos-sdk/issues/16173) for more details.

There is a lot of legacy code in the SDK that is not used anymore and can be removed. See the [Discussion](https://github.com/cosmos/iavl/issues/737) for more details. 

## Context

The introduction of `Store V2` separates SS (State Storage) and SC (State Commitment), where `iavl` is used for SC and `versionDB` is already implemented for SS. The index of the `fast node` is not needed in `iavl` since `versionDB` allows key-value queries for a specific version.
Additionally, `Store V2` introduces a new approach to `SaveVersion` that accepts a batch object for the atomicity of the commit and removes the usage of `dbm.Batch` in `nodeDB`.

The current implementation of `iavl` suffers from performance issues due to synchronous writes during the `Commit` process. To address this, the proposed changes aim to finalize the current version in memory, parallelize hash calculations in `WorkingHash`, and introduce asynchronous writes.

Moreover, the existing architecture of `iavl` lacks modularity and code organization:

- The boundary between `ImmutableTree` and `MutableTree` is not clear.
- There are many public methods in `nodeDB`, making it less structured.
- `nodeDB` serves as both storage and cache simultaneously.

## Decision

To support `Store V2` and improve `Commit` performance, we propose the following changes:

### Support Store V2

- Refactor the `SaveVersion` function to accept a batch object.
- Remove the index of the `fast node` from `iavl`.
- Eliminate the usage of `dbm.Batch` in `nodeDB`.

### Async Commit

- Parallelize hash calculations in `WorkingHash`.
- Finalize the current version in memory during `Commit`.
- Perform async writes in the background.

### API Cleanup

- Merge `ImmutableTree` and `MutableTree` into a single `Tree` structure.
- Make `nodeDB` methods private.
- Separate the cache from `nodeDB` and introduce a new `nodeCache` component.

The modified API will have the following structure:

```go
    Tree interface {
        Has(key []byte) (bool, error)
        Get(key []byte) ([]byte, error)
        SaveVersion(cs *ChangeSet) ([]byte, int64, error)
        Version() int64
        Hash() []byte
        WorkingHash() []byte
        VersionExists(version int64) bool
        DeleteVersionsTo(version int64) error
        GetVersioned(key []byte, version int64) ([]byte, *cmtprotocrypto.ProofOps, error)
        GetTree(version int64) (*iavl.Tree, error)
        SetInitialVersion(version uint64)
        Iterator(start, end []byte, ascending bool) (types.Iterator, error)
        LoadVersionForOverwriting(targetVersion int64) error
        WaitForCommit() error // this is for when gracefully shutdown
    }
```
## Consequences

We expect the proposed changes to improve the performance of Commit through async writes and parallelized hash calculations. Additionally, the codebase will become more organized and maintainable.

### Backwards Compatibility

While this ADR will break the external API of `iavl`, it will not affect the internal state of nodeDB. Compatibility measures and migration steps will be necessary during the `Store V2` migration to handle the breaking changes.

### Positive

- Atomicity of the `Commit`.
- Improved Commit performance through async writes and parallelized hash calculations.
- Increased flexibility and ease of modification and refactoring for iavl.

### Negative

- Async Commit may result in increased memory usage and increased code complexity.

## References

- [Store V2](https://github.com/cosmos/cosmos-sdk/blob/main/docs/architecture/adr-065-store-v2.md)
- https://github.com/cosmos/cosmos-sdk/issues/16173
- https://github.com/cosmos/iavl/issues/737