package app

import (
	"fmt"
	"io"
	"os"
	"path"

	abci "github.com/tendermint/abci/types"
	. "github.com/tendermint/go-common"
	dbm "github.com/tendermint/go-db"
	"github.com/tendermint/go-merkle"
	"github.com/tendermint/go-wire"
)

type MerkleEyesApp struct {
	state      State
	db         dbm.DB
	persistent bool
}

var saveKey []byte = []byte{0x00} //Key for merkle tree save value db values
var prefix []byte = []byte{0x01}  //Prefix byte for all data saved in the merkle tree, used to prevent key collision with the savekey record

func NewMerkleEyesApp(dbName string, cache int) *MerkleEyesApp {

	//non-persistent case
	if dbName == "" {
		tree := merkle.NewIAVLTree(
			0,
			nil,
		)
		return &MerkleEyesApp{
			state:      NewState(tree),
			db:         nil,
			persistent: false,
		}
	}

	//setup the persistent merkle tree
	present, _ := IsDirEmpty(path.Join(dbName, dbName+".db"))

	//open the db, if the db doesn't exist it will be created
	db := dbm.NewDB(dbName, dbm.DBBackendLevelDB, dbName)

	// Load Tree
	tree := merkle.NewIAVLTree(cache, db)

	if present {
		fmt.Println("no existing db, creating new db")
		db.Set(saveKey, tree.Save())
	} else {
		fmt.Println("loading existing db")
	}

	//load merkle state
	tree.Load(db.Get(saveKey))

	return &MerkleEyesApp{
		state:      NewState(tree),
		tree:       tree,
		db:         db,
		persistent: true,
	}
}

//append the prefix before the key, used to prevent db collisions
func AddPrefix(key []byte) []byte {
	return append(prefix, key...)
}

func IsDirEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return true, err //folder is non-existent
	}
	defer f.Close()

	_, err = f.Readdirnames(1) // Or f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err // Either not empty or error, suits both cases
}

func (app *MerkleEyesApp) CloseDb() {
	app.db.Close()
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
			return abci.ErrEncodingError.SetLog(Fmt("Error getting key: %v", err.Error()))
		}
		tx = tx[n:]
		value, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return abci.ErrEncodingError.SetLog(Fmt("Error getting value: %v", err.Error()))
		}
		tx = tx[n:]
		if len(tx) != 0 {
			return abci.ErrEncodingError.SetLog(Fmt("Got bytes left over"))
		}

		tree.Set(AddPrefix(key), value)
		fmt.Println("SET", Fmt("%X", key), Fmt("%X", value))
	case 0x02: // Remove
		key, n, err := wire.GetByteSlice(tx)
		if err != nil {
			return abci.ErrEncodingError.SetLog(Fmt("Error getting key: %v", err.Error()))
		}
		tx = tx[n:]
		if len(tx) != 0 {
			return abci.ErrEncodingError.SetLog(Fmt("Got bytes left over"))
		}
		tree.Remove(AddPrefix(key))
	default:
		return abci.ErrUnknownRequest.SetLog(Fmt("Unexpected Tx type byte %X", typeByte))
	}
	return abci.OK
}

func (app *MerkleEyesApp) Commit() abci.Result {

	hash := app.state.Commit()

	if app.persistent {
		app.db.Set(saveKey, app.tree.Save())
	}

	if app.state.Committed().Size() == 0 {
		return abci.NewResultOK(nil, "Empty hash for empty tree")
	}
	return abci.NewResultOK(hash, "")
}

func (app *MerkleEyesApp) Query(query []byte) abci.Result {
	if len(query) == 0 {
		return abci.OK
	}
	tree := app.state.Committed()

	typeByte := query[0]
	query = query[1:]
	switch typeByte {
	case 0x01: // Get by key
		key, n, err := wire.GetByteSlice(query)
		if err != nil {
			return abci.ErrEncodingError.SetLog(Fmt("Error getting key: %v", err.Error()))
		}
		query = query[n:]
		if len(query) != 0 {
			return abci.ErrEncodingError.SetLog(Fmt("Got bytes left over"))
		}
		_, value, _ := tree.Get(addPrefix(key))
		return abci.NewResultOK(value, "")
	case 0x02: // Get by index
		index, n, err := wire.GetVarint(query)
		if err != nil {
			return abci.ErrEncodingError.SetLog(Fmt("Error getting index: %v", err.Error()))
		}
		query = query[n:]
		if len(query) != 0 {
			return abci.ErrEncodingError.SetLog(Fmt("Got bytes left over"))
		}
		_, value := tree.GetByIndex(index)
		return abci.NewResultOK(value, "")
	case 0x03: // Get size
		size := tree.Size()
		res := wire.BinaryBytes(size)
		return abci.NewResultOK(res, "")
	default:
		return abci.ErrUnknownRequest.SetLog(Fmt("Unexpected Query type byte %X", typeByte))
	}
}

// Proof fulfills the ABCI app interface. key is the one for which we
// request a proof.  blockHeight is the height for which we want the proof.
// If blockHeight is 0, return the last commit.
func (app *MerkleEyesApp) Proof(key []byte, blockHeight uint64) abci.Result {
	// TODO: support older commits - right now we don't save the info
	if blockHeight != 0 {
		return abci.ErrInternalError.SetLog("merkleeyes only supports proofs on latest commit")
	}

	proof, exists := app.state.Committed().Proof(key)
	if !exists {
		return abci.NewResultOK(nil, "Key not found")
	}
	return abci.NewResultOK(proof, "")
}
