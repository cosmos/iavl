package types

import tmsp "github.com/tendermint/tmsp/types"

type MerkleEyser interface {
	GetSync(key []byte) tmsp.Result
	SetSync(key []byte, value []byte) tmsp.Result
	RemSync(key []byte) tmsp.Result
	CommitSync() tmsp.Result
}
