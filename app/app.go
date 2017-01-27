package app

import (
	"fmt"

	abci "github.com/tendermint/abci/types"
	. "github.com/tendermint/go-common"
	"github.com/tendermint/go-merkle"
	"github.com/tendermint/go-wire"
)

type MerkleEyesApp struct {
	state State
}

func NewMerkleEyesApp() *MerkleEyesApp {
	tree := merkle.NewIAVLTree(
		0,
		nil,
	)
	return &MerkleEyesApp{state: NewState(tree)}
}

func (app *MerkleEyesApp) Info() abci.ResponseInfo {
	return abci.ResponseInfo{Data: Fmt("size:%v", app.state.Committed().Size())}
}

func (app *MerkleEyesApp) SetOption(key string, value string) (log string) {
	return "No options are supported yet"
}

func (app *MerkleEyesApp) DeliverTx(tx []byte) abci.Result {
	tree := app.state.Append()
	return app.DoTx(tree, tx)
}

func (app *MerkleEyesApp) CheckTx(tx []byte) abci.Result {
	tree := app.state.Check()
	return app.DoTx(tree, tx)
}

func (app *MerkleEyesApp) DoTx(tree merkle.Tree, tx []byte) abci.Result {
	if len(tx) == 0 {
		return abci.ErrEncodingError.SetLog("Tx length cannot be zero")
	}
	typeByte := tx[0]
	tx = tx[1:]
	switch typeByte {
	case 0x01: // Set
		key, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return abci.ErrEncodingError.SetLog(Fmt("Error reading key: %v", err.Error()))
		}
		tx = tx[n:]
		value, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return abci.ErrEncodingError.SetLog(Fmt("Error reading value: %v", err.Error()))
		}
		tx = tx[n:]
		if len(tx) != 0 {
			return abci.ErrEncodingError.SetLog(Fmt("Got bytes left over"))
		}
		tree.Set(key, value)
		fmt.Println("SET", Fmt("%X", key), Fmt("%X", value))
	case 0x02: // Remove
		key, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return abci.ErrEncodingError.SetLog(Fmt("Error reading key: %v", err.Error()))
		}
		tx = tx[n:]
		if len(tx) != 0 {
			return abci.ErrEncodingError.SetLog(Fmt("Got bytes left over"))
		}
		tree.Remove(key)
	default:
		return abci.ErrUnknownRequest.SetLog(Fmt("Unexpected Tx type byte %X", typeByte))
	}
	return abci.OK
}

func (app *MerkleEyesApp) Commit() abci.Result {
	hash := app.state.Commit()
	if app.state.Committed().Size() == 0 {
		return abci.NewResultOK(nil, "Empty hash for empty tree")
	}
	return abci.NewResultOK(hash, "")
}

func (app *MerkleEyesApp) Query(reqQuery abci.RequestQuery) (resQuery abci.ResponseQuery) {
	if len(reqQuery.Data) == 0 {
		return
	}
	tree := app.state.Committed()

	if reqQuery.Height != 0 {
		// TODO: support older commits
		resQuery.Code = abci.CodeType_InternalError
		resQuery.Log = "merkleeyes only supports queries on latest commit"
		return
	}

	switch reqQuery.Path {
	case "/key": // Get by key
		key := reqQuery.Data // Data holds the key bytes
		if reqQuery.Prove {
			value, proof, exists := tree.Proof(key)
			if !exists {
				return abci.ResponseQuery{
					Log: "Key not found",
				}
			}
			// TODO: return index too?
			return abci.ResponseQuery{
				Key:   key,
				Value: value,
				Proof: proof,
			}
		} else {
			index, value, _ := tree.Get(key)
			return abci.ResponseQuery{
				Key:   key,
				Value: value,
				Index: int64(index),
			}
		}

	case "/index": // Get by Index
		key := reqQuery.Data // Data holds the key bytes
		index := wire.GetInt64(reqQuery.Data)
		key, value := tree.GetByIndex(int(index))
		return abci.ResponseQuery{
			Key:   key,
			Value: value,
			Index: int64(index),
		}

	case "/size": // Get size
		size := tree.Size()
		sizeBytes := wire.BinaryBytes(size)
		return abci.ResponseQuery{Value: sizeBytes}

	default:
		resQuery.Code = abci.CodeType_UnknownRequest
		resQuery.Log = Fmt("Unexpected Query path: %v", reqQuery.Path)
		return
	}
}
