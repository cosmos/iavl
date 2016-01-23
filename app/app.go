package app

import (
	. "github.com/tendermint/go-common"
	"github.com/tendermint/go-merkle"
	"github.com/tendermint/go-wire"
	"github.com/tendermint/tmsp/types"
)

type MerkleEyesApp struct {
	tree merkle.Tree
}

func NewMerkleEyesApp() *MerkleEyesApp {
	tree := merkle.NewIAVLTree(
		0,
		nil,
	)
	return &MerkleEyesApp{tree: tree}
}

func (app *MerkleEyesApp) Echo(message string) string {
	return message
}

func (app *MerkleEyesApp) Info() []string {
	return []string{Fmt("size:%v", app.tree.Size())}
}

func (app *MerkleEyesApp) SetOption(key string, value string) types.RetCode {
	return types.RetCodeOK
}

func (app *MerkleEyesApp) AppendTx(tx []byte) ([]types.Event, types.RetCode) {
	if len(tx) == 0 {
		return nil, types.RetCodeEncodingError
	}
	typeByte := tx[0]
	tx = tx[1:]
	switch typeByte {
	case 0x01: // Set
		key, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return nil, types.RetCodeEncodingError
		}
		tx = tx[n:]
		value, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return nil, types.RetCodeEncodingError
		}
		tx = tx[n:]
		if len(tx) != 0 {
			return nil, types.RetCodeEncodingError
		}
		app.tree.Set(key, value)
	case 0x02: // Remove
		key, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return nil, types.RetCodeEncodingError
		}
		tx = tx[n:]
		if len(tx) != 0 {
			return nil, types.RetCodeEncodingError
		}
		app.tree.Remove(key)
	}
	return nil, types.RetCodeOK
}

func (app *MerkleEyesApp) CheckTx(tx []byte) types.RetCode {
	if len(tx) == 0 {
		return types.RetCodeEncodingError
	}
	typeByte := tx[0]
	tx = tx[1:]
	switch typeByte {
	case 0x01: // Set
		_, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return types.RetCodeEncodingError
		}
		tx = tx[n:]
		_, n, err = wire.GetByteSlice(tx)
		if err != nil {
			return types.RetCodeEncodingError
		}
		tx = tx[n:]
		if len(tx) != 0 {
			return types.RetCodeEncodingError
		}
		//app.tree.Set(key, value)
	case 0x02: // Remove
		_, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return types.RetCodeEncodingError
		}
		tx = tx[n:]
		if len(tx) != 0 {
			return types.RetCodeEncodingError
		}
		//app.tree.Remove(key)
	default:
		return types.RetCodeUnknownRequest
	}
	return types.RetCodeOK
}

func (app *MerkleEyesApp) GetHash() ([]byte, types.RetCode) {
	hash := app.tree.Hash()
	return hash, types.RetCodeOK
}

func (app *MerkleEyesApp) AddListener(key string) types.RetCode {
	return types.RetCodeUnknownRequest
}

func (app *MerkleEyesApp) RemListener(key string) types.RetCode {
	return types.RetCodeUnknownRequest
}

func (app *MerkleEyesApp) Query(query []byte) ([]byte, types.RetCode) {
	if len(query) == 0 {
		return nil, types.RetCodeEncodingError
	}
	typeByte := query[0]
	query = query[1:]
	switch typeByte {
	case 0x01: // Get
		key, n, err := wire.GetByteSlice(query)
		if err != nil {
			return nil, types.RetCodeEncodingError
		}
		query = query[n:]
		if len(query) != 0 {
			return nil, types.RetCodeEncodingError
		}
		index, value, exists := app.tree.Get(key)
		res := make([]byte, wire.UvarintSize(uint64(index))+wire.UvarintSize(uint64(len(value)))+len(value)+1)
		n, err = wire.PutVarint(res, index)
		if err != nil {
			return nil, types.RetCodeInternalError
		}
		res = res[n:]
		n, err = wire.PutByteSlice(res, value)
		if err != nil {
			return nil, types.RetCodeInternalError
		}
		res = res[n:]
		if exists {
			res[0] = 0x01
		} else {
			res[0] = 0x00
		}
		return nil, types.RetCodeOK
	default:
		return nil, types.RetCodeUnknownRequest
	}
}
