## 0.12.4

\*\*

Special thanks to external contributors on this release:

### BREAKING CHANGES

- [/#158] NodeDB constructor must provide `keepRecent` and `keepEvery` fields to define PruningStrategy. All Save functionality must specify whether they should flushToDisk as well using `flushToDisk` boolean argument. All Delete functionality must specify whether object should be deleted from memory only using the `memOnly` boolean argument.

### IMPROVEMENTS

- [/#46](https://github.com/tendermint/iavl/issues/46) Removal of all instances of cmn from tendermint in Ival repo

- [/#158] Add constructor to IAVL that can define a pruning strategy. Only persist to disk snapshot versions and keep a specified number of versions in memDB. This greatly reduces IO load of IAVL.
