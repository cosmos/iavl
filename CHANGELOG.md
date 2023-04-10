# Changelog

<<<<<<< HEAD
=======
## Unreleased

### Improvements

- [#703](https://github.com/cosmos/iavl/pull/703) New APIs `NewCompressExporter`/`NewCompressImporter` to support more compact snapshot format.
- [#726](https://github.com/cosmos/iavl/pull/726) Make `KVPair` and `ChangeSet` serializable with protobuf.

### Breaking Changes

- [#646](https://github.com/cosmos/iavl/pull/646) Remove the `orphans` from the storage

### API Changes

- [#646](https://github.com/cosmos/iavl/pull/646) Remove the `DeleteVersion`, `DeleteVersions`, `DeleteVersionsRange` and introduce a new endpoint of `DeleteVersionsTo` instead
- [#695](https://github.com/cosmos/iavl/pull/695) Add API `SaveChangeSet` to save the changeset as a new version.

>>>>>>> 61be28a (feat: make ChangeSet and KVPair protobuf serializable (#726))
## 0.20.0 (March 14, 2023)

- [#622](https://github.com/cosmos/iavl/pull/622) `export/newExporter()` and `ImmutableTree.Export()` returns error for nil arguements
- [#586](https://github.com/cosmos/iavl/pull/586) Remove the `RangeProof` and refactor the ics23_proof to use the internal methods.
- [#640](https://github.com/cosmos/iavl/pull/640) commit `NodeDB` batch in `LoadVersionForOverwriting`.
- [#636](https://github.com/cosmos/iavl/pull/636) Speed up rollback method: `LoadVersionForOverwriting`.
- [#638](https://github.com/cosmos/iavl/pull/638) Make LazyLoadVersion check the opts.InitialVersion, add API `LazyLoadVersionForOverwriting`.
