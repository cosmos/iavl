package iavl

import (
	"io/ioutil"
	"testing"

	"github.com/tendermint/tmlibs/common"
	"github.com/tendermint/tmlibs/db"
)

func TestIAVLTreeFdump(t *testing.T) {
	t.Skipf("Tree dump and DB code seem buggy so this test always crashes. See https://github.com/tendermint/tmlibs/issues/36")
	db := db.NewDB("test", db.MemDBBackendStr, "")
	tree := NewVersionedTree(100000, db)
	v := uint64(1)
	for i := 0; i < 1000000; i++ {
		tree.Set(i2b(int(common.RandInt32())), nil)
		if i > 990000 && i%1000 == 999 {
			tree.SaveVersion(v)
			v++
		}
	}
	tree.SaveVersion(v)

	// insert lots of info and store the bytes
	for i := 0; i < 200; i++ {
		key, value := common.RandStr(20), common.RandStr(200)
		tree.Set([]byte(key), []byte(value))
	}

	tree.Fdump(ioutil.Discard, true, nil)
}
