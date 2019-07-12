## 0.12.3

\*\*

Special thanks to external contributors on this release:

### IMPROVEMENTS

- Implement LazyLoadVersion (@alexanderbez)
  - LazyLoadVersion attempts to lazy load only the specified target version
    without loading previous roots/versions. - see [goDoc](https://godoc.org/github.com/tendermint/iavl#MutableTree.LazyLoadVersion)
- Move to go.mod(@Liamsi)
- `Iaviewer` command to visualize iavl database from leveldb(@ethanfrey)
