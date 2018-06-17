package iavl

import (
	amino "github.com/tendermint/go-amino"
)

var cdc = amino.NewCodec()

func init() {
	// NOTE: It's important that there be no conflicts here,
	// as that would change the canonical representations.
	RegisterAmino(cdc)
}

func RegisterAmino(cdc *amino.Codec) {
	// TODO
}
