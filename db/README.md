# DB

The `db` package contains the key-value database interface and `memdb` implementation. The `memdb` is a simple in-memory key-value store that is used for testing and development purposes.

```go
package main

import (
    "cosmossdk.io/log"

    "github.com/cosmos/iavl"
    dbm "github.com/cosmos/iavl/db"
)

func main() {
    levelDB, err := dbm.NewGoLevelDB("application", "test")
    if err != nil {
        panic(err)
    }

    tree := iavl.NewMutableTree(dbm.NewPrefixDB(levelDB, []byte("s/k:main/")), 0, false, NewNopLogger())
}
```
