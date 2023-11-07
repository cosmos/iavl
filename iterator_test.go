package iavl_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cosmos/iavl/v2"
	"github.com/stretchr/testify/require"
)

func Test_Iterator(t *testing.T) {
	pool := iavl.NewNodePool()
	sql, err := iavl.NewInMemorySqliteDb(pool)
	require.NoError(t, err)

	tree := iavl.NewTree(sql, pool, iavl.TreeOptions{StateStorage: true})
	set := func(key string, value string) {
		_, err := tree.Set([]byte(key), []byte(value))
		require.NoError(t, err)
	}
	set("a", "1")
	set("b", "2")
	set("c", "3")
	set("d", "4")
	set("e", "5")
	set("f", "6")
	set("g", "7")

	itr, err := tree.Iterator(nil, nil, true)
	require.NoError(t, err)
	itr.Next()
	require.NoError(t, itr.Error())
	require.Equal(t, []byte("a"), itr.Key())
	require.Equal(t, []byte("1"), itr.Value())

	cnt := 0
	for ; itr.Valid(); itr.Next() {
		require.NoError(t, itr.Error())
		cnt++
	}
	require.Equal(t, 7, cnt)
	require.Equal(t, []byte("g"), itr.Key())
	require.Equal(t, []byte("7"), itr.Value())
	require.False(t, itr.Valid())
	require.NoError(t, itr.Close())

	itr, err = tree.Iterator([]byte("b"), nil, true)
	require.NoError(t, err)
	itr.Next()
	require.NoError(t, itr.Error())
	require.Equal(t, []byte("b"), itr.Key())
	cnt = 0
	for ; itr.Valid(); itr.Next() {
		require.NoError(t, itr.Error())
		fmt.Println(string(itr.Key()), string(itr.Value()))
		cnt++
	}
	require.Equal(t, 6, cnt)

	itr, err = tree.Iterator([]byte("ab"), nil, true)
	require.NoError(t, err)
	itr.Next()
	require.NoError(t, itr.Error())
	require.Equal(t, []byte("b"), itr.Key())
	cnt = 0
	for ; itr.Valid(); itr.Next() {
		require.NoError(t, itr.Error())
		fmt.Println(string(itr.Key()), string(itr.Value()))
		cnt++
	}
	require.Equal(t, 6, cnt)
}

func Test_Compare(t *testing.T) {
	res := bytes.Compare([]byte("a"), []byte("b"))
	require.Negative(t, res)
	res = bytes.Compare(nil, []byte("a"))
	require.Negative(t, res)
}
