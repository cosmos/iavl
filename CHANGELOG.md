# Changelog

## Unreleased

### Improvements

<<<<<<< HEAD
=======
- [#703](https://github.com/cosmos/iavl/pull/703) New APIs `NewCompressExporter`/`NewCompressImporter` to support more compact snapshot format.
- [#729](https://github.com/cosmos/iavl/pull/729) Speedup Genesis writes for IAVL, by writing in small batches.
>>>>>>> 747bc12 (perf: Make ResetBatch accumulate a minimum batch size (#729))
- [#726](https://github.com/cosmos/iavl/pull/726) Make `KVPair` and `ChangeSet` serializable with protobuf.

## 0.20.0 (March 14, 2023)

- [#622](https://github.com/cosmos/iavl/pull/622) `export/newExporter()` and `ImmutableTree.Export()` returns error for nil arguements
- [#586](https://github.com/cosmos/iavl/pull/586) Remove the `RangeProof` and refactor the ics23_proof to use the internal methods.
- [#640](https://github.com/cosmos/iavl/pull/640) commit `NodeDB` batch in `LoadVersionForOverwriting`.
- [#636](https://github.com/cosmos/iavl/pull/636) Speed up rollback method: `LoadVersionForOverwriting`.
- [#638](https://github.com/cosmos/iavl/pull/638) Make LazyLoadVersion check the opts.InitialVersion, add API `LazyLoadVersionForOverwriting`.
