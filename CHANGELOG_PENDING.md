## 0.12.5

\*\*

Special thanks to external contributors on this release:
@rickyyangz

### BREAKING CHANGES

- [/#158] NodeDB constructor must provide `keepRecent` and `keepEvery` fields to define PruningStrategy. All Save functionality must specify whether they should flushToDisk as well using `flushToDisk` boolean argument. All Delete functionality must specify whether object should be deleted from memory only using the `memOnly` boolean argument.

### IMPROVEMENTS

### Bug Fix

- [#177](https://github.com/tendermint/iavl/pull/177) Collect all orphans after remove (@rickyyangz)
