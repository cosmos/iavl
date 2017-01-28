package app

import (
	"fmt"
	"path"

	abci "github.com/tendermint/abci/types"
	cmn "github.com/tendermint/go-common"
	dbm "github.com/tendermint/go-db"
	"github.com/tendermint/go-merkle"
	"github.com/tendermint/go-wire"
)

type MerkleEyesApp struct {
	state State
	db    dbm.DB
}

var saveKey []byte = []byte{0x00} // Database key for merkle tree save value db values

const (
	WriteSet byte = 0x01
	WriteRem byte = 0x02
)

func NewMerkleEyesApp(dbName string, cacheSize int) *MerkleEyesApp {

	// Non-persistent case
	if dbName == "" {
		tree := merkle.NewIAVLTree(
			0,
			nil,
		)
		return &MerkleEyesApp{
			state: NewState(tree, false),
			db:    nil,
		}
	}

	// Setup the persistent merkle tree
	empty, _ := cmn.IsDirEmpty(path.Join(dbName, dbName+".db"))

	// Open the db, if the db doesn't exist it will be created
	db := dbm.NewDB(dbName, dbm.LevelDBBackendStr, dbName)

	// Load Tree
	tree := merkle.NewIAVLTree(cacheSize, db)

	if empty {
		fmt.Println("no existing db, creating new db")
		db.Set(saveKey, tree.Save())
	} else {
		fmt.Println("loading existing db")
	}

	// Load merkle state
	tree.Load(db.Get(saveKey))

	return &MerkleEyesApp{
		state: NewState(tree, true),
		db:    db,
	}
}

func (app *MerkleEyesApp) CloseDB() {
	if app.db != nil {
		app.db.Close()
	}
}

func (app *MerkleEyesApp) Info() abci.ResponseInfo {
	return abci.ResponseInfo{Data: cmn.Fmt("size:%v", app.state.Committed().Size())}
}

func (app *MerkleEyesApp) SetOption(key string, value string) (log string) {
	return "No options are supported yet"
}

func (app *MerkleEyesApp) DeliverTx(tx []byte) abci.Result {
	tree := app.state.Append()
	return app.doTx(tree, tx)
}

func (app *MerkleEyesApp) CheckTx(tx []byte) abci.Result {
	tree := app.state.Check()
	return app.doTx(tree, tx)
}

func (app *MerkleEyesApp) doTx(tree merkle.Tree, tx []byte) abci.Result {
	if len(tx) == 0 {
		return abci.ErrEncodingError.SetLog("Tx length cannot be zero")
	}
	typeByte := tx[0]
	tx = tx[1:]
	switch typeByte {
	case WriteSet: // Set
		key, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return abci.ErrEncodingError.SetLog(cmn.Fmt("Error reading key: %v", err.Error()))
		}
		tx = tx[n:]
		value, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return abci.ErrEncodingError.SetLog(cmn.Fmt("Error reading value: %v", err.Error()))
		}
		tx = tx[n:]
		if len(tx) != 0 {
			return abci.ErrEncodingError.SetLog(cmn.Fmt("Got bytes left over"))
		}

		tree.Set(key, value)
		fmt.Println("SET", cmn.Fmt("%X", key), cmn.Fmt("%X", value))
	case WriteRem: // Remove
		key, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return abci.ErrEncodingError.SetLog(cmn.Fmt("Error reading key: %v", err.Error()))
		}
		tx = tx[n:]
		if len(tx) != 0 {
			return abci.ErrEncodingError.SetLog(cmn.Fmt("Got bytes left over"))
		}
		tree.Remove(key)
	default:
		return abci.ErrUnknownRequest.SetLog(cmn.Fmt("Unexpected Tx type byte %X", typeByte))
	}
	return abci.OK
}

func (app *MerkleEyesApp) Commit() abci.Result {

	hash := app.state.Commit()

	if app.db != nil {
		app.db.Set(saveKey, hash)
	}

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
		resQuery.Log = cmn.Fmt("Unexpected Query path: %v", reqQuery.Path)
		return
	}
}
