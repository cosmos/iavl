package iavl_test

import (
	"bytes"
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

	cases := []struct {
		name          string
		start, end    []byte
		ascending     bool
		expectedCount int
		expectedStart []byte
		expectedEnd   []byte
	}{
		{
			name:          "all",
			start:         nil,
			end:           nil,
			ascending:     true,
			expectedCount: 7,
			expectedStart: []byte("a"),
			expectedEnd:   []byte("g"),
		},
		{
			name:          "b start",
			start:         []byte("b"),
			end:           nil,
			ascending:     true,
			expectedCount: 6,
			expectedStart: []byte("b"),
			expectedEnd:   []byte("g"),
		},
		{
			name:          "ab start",
			start:         []byte("ab"),
			end:           nil,
			ascending:     true,
			expectedCount: 6,
			expectedStart: []byte("b"),
			expectedEnd:   []byte("g"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			itr, err := tree.Iterator(tc.start, tc.end, tc.ascending)
			require.NoError(t, err)
			itr.Next()
			require.NoError(t, itr.Error())
			require.Equal(t, tc.expectedStart, itr.Key())

			cnt := 0
			for ; itr.Valid(); itr.Next() {
				require.NoError(t, itr.Error())
				cnt++
			}
			require.Equal(t, tc.expectedCount, cnt)
			require.Equal(t, tc.expectedEnd, itr.Key())
			require.False(t, itr.Valid())
			require.NoError(t, itr.Close())
		})
	}
}

func Test_Compare(t *testing.T) {
	res := bytes.Compare([]byte("a"), []byte("b"))
	require.Negative(t, res)
	res = bytes.Compare(nil, []byte("a"))
	require.Negative(t, res)
}
