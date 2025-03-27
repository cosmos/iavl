package iavl_test

import (
	"fmt"
	"testing"

	"github.com/cosmos/iavl/v2"
	"github.com/stretchr/testify/require"
)

func Test_Iterator(t *testing.T) {
	pool := iavl.NewNodePool()
	sql, err := iavl.NewInMemorySqliteDb(pool)
	require.NoError(t, err)

	opts := iavl.DefaultTreeOptions()
	tree := iavl.NewTree(sql, pool, opts)
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
		inclusive     bool
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
		{
			name:          "c end inclusive",
			start:         nil,
			end:           []byte("c"),
			ascending:     true,
			inclusive:     true,
			expectedCount: 3,
			expectedStart: []byte("a"),
			expectedEnd:   []byte("c"),
		},
		{
			name:          "d end exclusive",
			start:         nil,
			end:           []byte("d"),
			ascending:     true,
			inclusive:     false,
			expectedCount: 3,
			expectedStart: []byte("a"),
			expectedEnd:   []byte("c"),
		},
		{
			name:          "ce end inclusive",
			start:         nil,
			end:           []byte("c"),
			ascending:     true,
			inclusive:     true,
			expectedCount: 3,
			expectedStart: []byte("a"),
			expectedEnd:   []byte("c"),
		},
		{
			name:          "ce end exclusive",
			start:         nil,
			end:           []byte("ce"),
			ascending:     true,
			inclusive:     false,
			expectedCount: 3,
			expectedStart: []byte("a"),
			expectedEnd:   []byte("c"),
		},
		{
			name:          "b to e",
			start:         []byte("b"),
			end:           []byte("e"),
			inclusive:     true,
			ascending:     true,
			expectedCount: 4,
			expectedStart: []byte("b"),
			expectedEnd:   []byte("e"),
		},
		{
			name:          "all desc",
			start:         nil,
			end:           nil,
			ascending:     false,
			expectedCount: 7,
			expectedStart: []byte("g"),
			expectedEnd:   []byte("a"),
		},
		{
			name:          "f start desc",
			start:         nil,
			end:           []byte("f"),
			ascending:     false,
			expectedCount: 5,
			expectedStart: []byte("e"),
			expectedEnd:   []byte("a"),
		},
		{
			name:          "fe start desc",
			start:         nil,
			end:           []byte("fe"),
			ascending:     false,
			expectedCount: 6,
			expectedStart: []byte("f"),
			expectedEnd:   []byte("a"),
		},
		{
			name:          "c stop desc",
			start:         []byte("c"),
			end:           nil,
			ascending:     false,
			expectedCount: 5,
			expectedStart: []byte("g"),
			expectedEnd:   []byte("c"),
		},
		{
			name:          "ce stop desc",
			start:         []byte("ce"),
			end:           nil,
			ascending:     false,
			expectedCount: 4,
			expectedStart: []byte("g"),
			expectedEnd:   []byte("d"),
		},
		{
			name:          "f to c desc",
			start:         []byte("c"),
			end:           []byte("f"),
			ascending:     false,
			expectedCount: 3,
			expectedStart: []byte("e"),
			expectedEnd:   []byte("c"),
		},
		{
			name:          "fe to f should include f",
			start:         []byte("f"),
			end:           []byte("fe"),
			ascending:     false,
			expectedCount: 1,
			expectedStart: []byte("f"),
			expectedEnd:   []byte("f"),
		},
		{
			name:          "no range",
			start:         []byte("ce"),
			end:           []byte("cf"),
			ascending:     true,
			expectedCount: 0,
			expectedStart: nil,
			expectedEnd:   nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				itr iavl.Iterator
				err error
			)
			if tc.ascending {
				itr, err = tree.Iterator(tc.start, tc.end, tc.inclusive)
			} else {
				itr, err = tree.ReverseIterator(tc.start, tc.end)
			}
			require.NoError(t, err)

			if tc.expectedCount == 0 {
				require.False(t, itr.Valid())
			}

			cnt := 0
			for ; itr.Valid(); itr.Next() {
				if cnt == 0 {
					require.Equal(t, tc.expectedStart, itr.Key())
				}
				//fmt.Printf("%s %s\n", itr.Key(), itr.Value())
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

func Test_IteratorTree(t *testing.T) {
	tmpDir := t.TempDir()
	pool := iavl.NewNodePool()
	sql, err := iavl.NewSqliteDb(pool, iavl.SqliteDbOptions{Path: tmpDir})
	require.NoError(t, err)

	opts := iavl.DefaultTreeOptions()
	tree := iavl.NewTree(sql, pool, opts)
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

	_, version, err := tree.SaveVersion()
	require.NoError(t, err)
	tree = iavl.NewTree(sql, pool, opts)
	require.NoError(t, tree.LoadVersion(version))
	cases := []struct {
		name          string
		start, end    []byte
		inclusive     bool
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
			name:          "all desc",
			start:         nil,
			end:           nil,
			ascending:     false,
			expectedCount: 7,
			expectedStart: []byte("g"),
			expectedEnd:   []byte("a"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				itr iavl.Iterator
				err error
			)
			if tc.ascending {
				itr, err = tree.Iterator(tc.start, tc.end, tc.inclusive)
			} else {
				itr, err = tree.ReverseIterator(tc.start, tc.end)
			}
			require.NoError(t, err)

			one, err := tree.Get([]byte("a"))
			require.NoError(t, err)
			require.Equal(t, []byte("1"), one)

			cnt := 0
			for ; itr.Valid(); itr.Next() {
				if cnt == 0 {
					require.Equal(t, tc.expectedStart, itr.Key())
				}
				fmt.Printf("%s %s\n", itr.Key(), itr.Value())
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
