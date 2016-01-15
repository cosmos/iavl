package main

import (
	"bytes"

	"github.com/tendermint/go-wire"
	"github.com/tendermint/merkleeyes/types"

	. "github.com/tendermint/go-common"
)

func main() {
	var bz = make([]byte, 1024)
	copy(bz, []byte{1, 9, 0x01, 1, 6, 34, 34, 34, 34, 34, 34})
	var buf = bytes.NewBuffer(bz)
	var req types.Request

	for i := 0; i < 200000; i++ {
		{
			buf = bytes.NewBuffer(bz)
			var n int
			var err error
			wire.ReadBinaryPtrLengthPrefixed(&req, buf, 0, &n, &err)
			if err != nil {
				Exit(err.Error())
				return
			}
		}
		{
			buf = bytes.NewBuffer(bz)
			var n int
			var err error
			wire.WriteBinaryLengthPrefixed(struct{ types.Request }{req}, buf, &n, &err)
			if err != nil {
				Exit(err.Error())
				return
			}

		}
	}

}
