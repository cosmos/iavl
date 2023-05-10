# Changelog

## Unreleased

<<<<<<< HEAD
=======
### Improvements

- [#703](https://github.com/cosmos/iavl/pull/703) New APIs `NewCompressExporter`/`NewCompressImporter` to support more compact snapshot format.
- [#729](https://github.com/cosmos/iavl/pull/729) Speedup Genesis writes for IAVL, by writing in small batches.
- [#726](https://github.com/cosmos/iavl/pull/726) Make `KVPair` and `ChangeSet` serializable with protobuf.
- [#718](https://github.com/cosmos/iavl/pull/718) Fix `traverseNodes` unexpected behaviour

### Breaking Changes

- [#735](https://github.com/cosmos/iavl/pull/735) Pass logger to `NodeDB`, `MutableTree` and `ImmutableTree`

- [#646](https://github.com/cosmos/iavl/pull/646) Remove the `orphans` from the storage

### API Changes

- [#646](https://github.com/cosmos/iavl/pull/646) Remove the `DeleteVersion`, `DeleteVersions`, `DeleteVersionsRange` and introduce a new endpoint of `DeleteVersionsTo` instead
- [#695](https://github.com/cosmos/iavl/pull/695) Add API `SaveChangeSet` to save the changeset as a new version.

## 0.20.0 (March 14, 2023)

### Breaking Changes

- [#586](https://github.com/cosmos/iavl/pull/586) Remove the `RangeProof` and refactor the ics23_proof to use the internal methods.

## 0.19.5 (Februrary 23, 2022)

### Breaking Changes

- [#622](https://github.com/cosmos/iavl/pull/622) `export/newExporter()` and `ImmutableTree.Export()` returns error for nil arguements

- [#640](https://github.com/cosmos/iavl/pull/640) commit `NodeDB` batch in `LoadVersionForOverwriting`.
- [#636](https://github.com/cosmos/iavl/pull/636) Speed up rollback method: `LoadVersionForOverwriting`.
>>>>>>> 747bc12 (perf: Make ResetBatch accumulate a minimum batch size (#729))
- [#654](https://github.com/cosmos/iavl/pull/654) Add API `TraverseStateChanges` to extract state changes from iavl versions.
