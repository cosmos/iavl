# Changelog

## Unreleased

## 0.20.1 (September 5, 2023)

### Improvements

- [#654](https://github.com/cosmos/iavl/pull/654) Add API `TraverseStateChanges` to extract state changes from iavl versions.
- [#726](https://github.com/cosmos/iavl/pull/726) Make `KVPair` and `ChangeSet` serializable with protobuf.
- [#795](https://github.com/cosmos/iavl/pull/795) Use gogofaster buf plugin.

### Bug Fixes

- [#801](https://github.com/cosmos/iavl/pull/801) Fix rootKey empty check by len equals 0.
- [#829](https://github.com/cosmos/iavl/pull/829) Fix fastnode concurrency panic.

## 0.20.0 (March 14, 2023)

- [#622](https://github.com/cosmos/iavl/pull/622) `export/newExporter()` and `ImmutableTree.Export()` returns error for nil arguements
- [#586](https://github.com/cosmos/iavl/pull/586) Remove the `RangeProof` and refactor the ics23_proof to use the internal methods.
- [#640](https://github.com/cosmos/iavl/pull/640) commit `NodeDB` batch in `LoadVersionForOverwriting`.
- [#636](https://github.com/cosmos/iavl/pull/636) Speed up rollback method: `LoadVersionForOverwriting`.
- [#638](https://github.com/cosmos/iavl/pull/638) Make LazyLoadVersion check the opts.InitialVersion, add API `LazyLoadVersionForOverwriting`.
