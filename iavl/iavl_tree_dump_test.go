package iavl_test

import (
	"io/ioutil"
	"testing"

	"github.com/tendermint/go-wire"
	"github.com/tendermint/merkleeyes/iavl"
	"github.com/tendermint/tmlibs/common"
	"github.com/tendermint/tmlibs/db"
)

func i2b(i int) []byte {
	bz := make([]byte, 4)
	wire.PutInt32(bz, int32(i))
	return bz
}

func TestIAVLTreeFdump(t *testing.T) {
	db := db.NewDB("test", db.MemDBBackendStr, "")
	tree := iavl.NewIAVLTree(100000, db)
	for i := 0; i < 1000000; i++ {
		tree.Set(i2b(int(common.RandInt32())), nil)
		if i > 990000 && i%1000 == 999 {
			tree.Save()
		}
	}
	tree.Save()

	// insert lots of info and store the bytes
	for i := 0; i < 200; i++ {
		key, value := common.RandStr(20), common.RandStr(200)
		tree.Set([]byte(key), []byte(value))
	}

	tree.Fdump(ioutil.Discard, true, nil)
}
