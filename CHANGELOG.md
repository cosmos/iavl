# Changelog

## v1.3.2, November 26, 2024

### Bug Fixes

- [#1007](https://github.com/cosmos/iavl/pull/1007) Add the extra check for the reformatted root node in `GetNode`

## v1.3.0, July 31, 2024

### Improvements

- [#952](https://github.com/cosmos/iavl/pull/952) Add `DeleteVersionsFrom(int64)` API.
- [#955](https://github.com/cosmos/iavl/pull/955) Get rid of `cosmos-db` deps completely.
- [#961](https://github.com/cosmos/iavl/pull/961) Add new `GetLatestVersion` API to get the latest version.
- [#955](https://github.com/cosmos/iavl/pull/955) Get rid of `cosmos-db` deps completely.
- [#965](https://github.com/cosmos/iavl/pull/965) Use expected interface for expected IAVL `Logger`.
- [#970](https://github.com/cosmos/iavl/pull/970) Close the pruning process when the nodeDB is closed.
- [#980](https://github.com/cosmos/iavl/pull/980) Use the `sdk/core/store.KVStoreWithBatch` interface instead of `iavl/db.DB` interface
- [#1018](https://github.com/cosmos/iavl/pull/1018) Cache first version for legacy versions, fix performance regression after upgrade.
