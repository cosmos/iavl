package app

import (
	"fmt"
	. "github.com/tendermint/go-common"
	"github.com/tendermint/go-merkle"
	"github.com/tendermint/go-wire"
	tmsp "github.com/tendermint/tmsp/types"
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

func (app *MerkleEyesApp) Info() string {
	return Fmt("size:%v", app.tree.Size())
}

func (app *MerkleEyesApp) SetOption(key string, value string) (log string) {
	return "No options are supported yet"
}

func (app *MerkleEyesApp) AppendTx(tx []byte) (code tmsp.CodeType, result []byte, log string) {
	if len(tx) == 0 {
		return tmsp.CodeType_EncodingError, nil, "Tx length cannot be zero"
	}
	typeByte := tx[0]
	tx = tx[1:]
	switch typeByte {
	case 0x01: // Set
		key, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return tmsp.CodeType_EncodingError, nil, Fmt("Error getting key: %v", err.Error())
		}
		tx = tx[n:]
		value, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return tmsp.CodeType_EncodingError, nil, Fmt("Error getting value: %v", err.Error())
		}
		tx = tx[n:]
		if len(tx) != 0 {
			return tmsp.CodeType_EncodingError, nil, Fmt("Got bytes left over")
		}
		app.tree.Set(key, value)
		fmt.Println("SET", Fmt("%X", key), Fmt("%X", value))
	case 0x02: // Remove
		key, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return tmsp.CodeType_EncodingError, nil, Fmt("Error getting key: %v", err.Error())
		}
		tx = tx[n:]
		if len(tx) != 0 {
			return tmsp.CodeType_EncodingError, nil, Fmt("Got bytes left over")
		}
		app.tree.Remove(key)
	default:
		return tmsp.CodeType_UnknownRequest, nil, Fmt("Unexpected type byte %X", typeByte)
	}
	return tmsp.CodeType_OK, nil, ""
}

func (app *MerkleEyesApp) CheckTx(tx []byte) (code tmsp.CodeType, result []byte, log string) {
	if len(tx) == 0 {
		return tmsp.CodeType_EncodingError, nil, "Tx length cannot be zero"
	}
	typeByte := tx[0]
	tx = tx[1:]
	switch typeByte {
	case 0x01: // Set
		_, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return tmsp.CodeType_EncodingError, nil, Fmt("Error getting key: %v", err.Error())
		}
		tx = tx[n:]
		_, n, err = wire.GetByteSlice(tx)
		if err != nil {
			return tmsp.CodeType_EncodingError, nil, Fmt("Error getting value: %v", err.Error())
		}
		tx = tx[n:]
		if len(tx) != 0 {
			return tmsp.CodeType_EncodingError, nil, Fmt("Got bytes left over")
		}
		//app.tree.Set(key, value)
	case 0x02: // Remove
		_, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return tmsp.CodeType_EncodingError, nil, Fmt("Error getting key: %v", err.Error())
		}
		tx = tx[n:]
		if len(tx) != 0 {
			return tmsp.CodeType_EncodingError, nil, Fmt("Got bytes left over")
		}
		//app.tree.Remove(key)
	default:
		return tmsp.CodeType_UnknownRequest, nil, Fmt("Unexpected type byte %X", typeByte)
	}
	return tmsp.CodeType_OK, nil, ""
}

func (app *MerkleEyesApp) Commit() (hash []byte, log string) {
	if app.tree.Size() == 0 {
		return nil, "Empty hash for empty tree"
	}
	hash = app.tree.Hash()
	return hash, ""
}

func (app *MerkleEyesApp) Query(query []byte) (code tmsp.CodeType, result []byte, log string) {
	if len(query) == 0 {
		return tmsp.CodeType_OK, nil, "Query length cannot be zero"
	}
	typeByte := query[0]
	query = query[1:]
	switch typeByte {
	case 0x01: // Get
		key, n, err := wire.GetByteSlice(query)
		if err != nil {
			return tmsp.CodeType_EncodingError, nil, Fmt("Error getting key: %v", err.Error())
		}
		query = query[n:]
		if len(query) != 0 {
			return tmsp.CodeType_EncodingError, nil, Fmt("Got bytes left over")
		}
		_, value, _ := app.tree.Get(key)
		res := make([]byte, wire.ByteSliceSize(value))
		buf := res
		n, err = wire.PutByteSlice(buf, value)
		if err != nil {
			return tmsp.CodeType_EncodingError, nil, Fmt("Error putting value: %v", err.Error())
		}
		buf = buf[n:]
		return tmsp.CodeType_OK, res, ""
	default:
		return tmsp.CodeType_UnknownRequest, nil, Fmt("Unexpected type byte %X", typeByte)
	}
}
