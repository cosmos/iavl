package app

import (
	"fmt"

	. "github.com/tendermint/go-common"
	"github.com/tendermint/go-merkle"
	"github.com/tendermint/go-wire"
	tmsp "github.com/tendermint/tmsp/types"
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

func (app *MerkleEyesApp) Info() string {
	return Fmt("size:%v", app.state.Committed().Size())
}

func (app *MerkleEyesApp) SetOption(key string, value string) (log string) {
	return "No options are supported yet"
}

func (app *MerkleEyesApp) AppendTx(tx []byte) tmsp.Result {
	tree := app.state.Append()
	return app.DoTx(tree, tx)
}

func (app *MerkleEyesApp) CheckTx(tx []byte) tmsp.Result {
	tree := app.state.Check()
	return app.DoTx(tree, tx)
}

func (app *MerkleEyesApp) DoTx(tree merkle.Tree, tx []byte) tmsp.Result {
	if len(tx) == 0 {
		return tmsp.ErrEncodingError.SetLog("Tx length cannot be zero")
	}
	typeByte := tx[0]
	tx = tx[1:]
	switch typeByte {
	case 0x01: // Set
		key, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return tmsp.ErrEncodingError.SetLog(Fmt("Error getting key: %v", err.Error()))
		}
		tx = tx[n:]
		value, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return tmsp.ErrEncodingError.SetLog(Fmt("Error getting value: %v", err.Error()))
		}
		tx = tx[n:]
		if len(tx) != 0 {
			return tmsp.ErrEncodingError.SetLog(Fmt("Got bytes left over"))
		}
		tree.Set(key, value)
		fmt.Println("SET", Fmt("%X", key), Fmt("%X", value))
	case 0x02: // Remove
		key, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return tmsp.ErrEncodingError.SetLog(Fmt("Error getting key: %v", err.Error()))
		}
		tx = tx[n:]
		if len(tx) != 0 {
			return tmsp.ErrEncodingError.SetLog(Fmt("Got bytes left over"))
		}
		tree.Remove(key)
	default:
		return tmsp.ErrUnknownRequest.SetLog(Fmt("Unexpected Tx type byte %X", typeByte))
	}
	return tmsp.OK
}

func (app *MerkleEyesApp) Commit() tmsp.Result {
	hash := app.state.Commit()
	if app.state.Committed().Size() == 0 {
		return tmsp.NewResultOK(nil, "Empty hash for empty tree")
	}
	return tmsp.NewResultOK(hash, "")
}

func (app *MerkleEyesApp) Query(query []byte) tmsp.Result {
	if len(query) == 0 {
		return tmsp.OK
	}
	tree := app.state.Committed()

	typeByte := query[0]
	query = query[1:]
	switch typeByte {
	case 0x01: // Get by key
		key, n, err := wire.GetByteSlice(query)
		if err != nil {
			return tmsp.ErrEncodingError.SetLog(Fmt("Error getting key: %v", err.Error()))
		}
		query = query[n:]
		if len(query) != 0 {
			return tmsp.ErrEncodingError.SetLog(Fmt("Got bytes left over"))
		}
		_, value, _ := tree.Get(key)
		return tmsp.NewResultOK(value, "")
	case 0x02: // Get by index
		index, n, err := wire.GetVarint(query)
		if err != nil {
			return tmsp.ErrEncodingError.SetLog(Fmt("Error getting index: %v", err.Error()))
		}
		query = query[n:]
		if len(query) != 0 {
			return tmsp.ErrEncodingError.SetLog(Fmt("Got bytes left over"))
		}
		_, value := tree.GetByIndex(index)
		return tmsp.NewResultOK(value, "")
	case 0x03: // Get size
		size := tree.Size()
		res := wire.BinaryBytes(size)
		return tmsp.NewResultOK(res, "")
	default:
		return tmsp.ErrUnknownRequest.SetLog(Fmt("Unexpected Query type byte %X", typeByte))
	}
}

func (app *MerkleEyesApp) Proof(key []byte) tmsp.Result {
	// TODO: we really need to return some sort of error if not there... but what?
	proof, _ := app.state.Committed().Proof(key)
	return tmsp.NewResultOK(proof, "")
}
