# Changelog

## Unreleased

### Improvements

- [#654](https://github.com/cosmos/iavl/pull/654) Add API `TraverseStateChanges` to extract state changes from iavl versions.
- [#726](https://github.com/cosmos/iavl/pull/726) Make `KVPair` and `ChangeSet` serializable with protobuf.
<<<<<<< HEAD
- [#795](https://github.com/cosmos/iavl/pull/795) Use gogofaster buf plugin.
=======
- [#718](https://github.com/cosmos/iavl/pull/718) Fix `traverseNodes` unexpected behaviour
- [#770](https://github.com/cosmos/iavl/pull/770) Add `WorkingVersion()int64` API.

### Bug Fixes

- [#773](https://github.com/cosmos/iavl/pull/773) Fix memory leak in `Import`.
- [#795](https://github.com/cosmos/iavl/pull/795) Fix plugin used for buf generate.
- [#801](https://github.com/cosmos/iavl/pull/801) Fix rootKey empty check by len equals 0.

### Breaking Changes

- [#735](https://github.com/cosmos/iavl/pull/735) Pass logger to `NodeDB`, `MutableTree` and `ImmutableTree`

- [#646](https://github.com/cosmos/iavl/pull/646) Remove the `orphans` from the storage

- [#777](https://github.com/cosmos/iavl/pull/777) Don't return errors from ImmutableTree.Hash, NewImmutableTree, NewImmutableTreeWIthOpts

### API Changes

- [#646](https://github.com/cosmos/iavl/pull/646) Remove the `DeleteVersion`, `DeleteVersions`, `DeleteVersionsRange` and introduce a new endpoint of `DeleteVersionsTo` instead
- [#695](https://github.com/cosmos/iavl/pull/695) Add API `SaveChangeSet` to save the changeset as a new version.
>>>>>>> 06f5be1 (fix: rootKey empty check by len equals 0 (#801))

## 0.20.0 (March 14, 2023)

- [#622](https://github.com/cosmos/iavl/pull/622) `export/newExporter()` and `ImmutableTree.Export()` returns error for nil arguements
- [#586](https://github.com/cosmos/iavl/pull/586) Remove the `RangeProof` and refactor the ics23_proof to use the internal methods.
- [#640](https://github.com/cosmos/iavl/pull/640) commit `NodeDB` batch in `LoadVersionForOverwriting`.
- [#636](https://github.com/cosmos/iavl/pull/636) Speed up rollback method: `LoadVersionForOverwriting`.
- [#638](https://github.com/cosmos/iavl/pull/638) Make LazyLoadVersion check the opts.InitialVersion, add API `LazyLoadVersionForOverwriting`.
