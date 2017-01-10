package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	merkle "github.com/tendermint/go-merkle"
	wire "github.com/tendermint/go-wire"
)

func makeSet(key, value []byte) []byte {
	tx := make([]byte, 1+wire.ByteSliceSize(key)+wire.ByteSliceSize(value))
	buf := tx
	buf[0] = 0x01 // Set TypeByte
	buf = buf[1:]
	n, err := wire.PutByteSlice(buf, key)
	if err != nil {
		panic(err)
	}
	buf = buf[n:]
	n, err = wire.PutByteSlice(buf, value)
	if err != nil {
		panic(err)
	}
	return tx
}

func makeRemove(key []byte) []byte {
	tx := make([]byte, 1+wire.ByteSliceSize(key))
	buf := tx
	buf[0] = 0x02 // Set TypeByte
	buf = buf[1:]
	_, err := wire.PutByteSlice(buf, key)
	if err != nil {
		panic(err)
	}
	return tx
}

func makeQuery(key []byte) []byte {
	tx := make([]byte, 1+wire.ByteSliceSize(key))
	buf := tx
	buf[0] = 0x01 // Set TypeByte
	buf = buf[1:]
	_, err := wire.PutByteSlice(buf, key)
	if err != nil {
		panic(err)
	}
	return tx
}

func TestAppQueries(t *testing.T) {
	assert := assert.New(t)

	app := NewMerkleEyesApp()
	info := app.Info()
	assert.Equal("size:0", info)
	com := app.Commit()
	assert.Equal([]byte(nil), com.Data)

	// prepare some actions
	key, value := []byte("foobar"), []byte("works!")
	addTx := makeSet(key, value)
	removeTx := makeRemove(key)
	queryTx := makeQuery(key)

	// need to commit append before it shows in queries
	append := app.AppendTx(addTx)
	assert.True(append.IsOK(), append.Log)
	info = app.Info()
	assert.Equal("size:0", info)
	query := app.Query(queryTx)
	assert.True(query.IsOK(), query.Log)
	assert.Equal([]byte(nil), query.Data)

	com = app.Commit()
	hash := com.Data
	assert.NotEqual(t, nil, hash)
	info = app.Info()
	assert.Equal("size:1", info)
	query = app.Query(queryTx)
	assert.True(query.IsOK(), query.Log)
	assert.Equal(value, query.Data)

	// modifying check has no effect
	check := app.CheckTx(removeTx)
	assert.True(check.IsOK(), check.Log)
	com = app.Commit()
	assert.True(com.IsOK(), com.Log)
	hash2 := com.Data
	assert.Equal(hash, hash2)
	info = app.Info()
	assert.Equal("size:1", info)

	// proofs come from the last commited state, not working state
	append = app.AppendTx(removeTx)
	assert.True(append.IsOK(), append.Log)
	proof := app.Proof(key)
	if assert.NotEmpty(proof.Data) {
		loaded, err := merkle.LoadProof(proof.Data)
		if assert.Nil(err) {
			assert.True(loaded.Valid())
			assert.Equal(hash, loaded.Root())
			assert.Equal(key, loaded.Key())
			assert.Equal(value, loaded.Value())
		}
	}

	// commit remove actually removes it now
	com = app.Commit()
	assert.True(com.IsOK(), com.Log)
	hash3 := com.Data
	assert.NotEqual(hash, hash3)
	info = app.Info()
	assert.Equal("size:0", info)

	// nothing here...
	query = app.Query(queryTx)
	assert.True(query.IsOK(), query.Log)
	assert.Equal([]byte(nil), query.Data)
	proof = app.Proof(key)
	assert.Empty(proof.Data)
}
