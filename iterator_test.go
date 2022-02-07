package iavl

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	dbm "github.com/tendermint/tm-db"
)

func TestIterator_NewIterator_NilTree_Failure(t *testing.T) {
	var start, end []byte = []byte{'a'}, []byte{'c'}
	ascending := true

	performTest := func(t *testing.T, itr dbm.Iterator) {
		require.NotNil(t, itr)
		require.False(t, itr.Valid())
		actualsStart, actualEnd := itr.Domain()
		require.Equal(t, start, actualsStart)
		require.Equal(t, end, actualEnd)
		require.Error(t, itr.Error())
	}

	t.Run("Iterator", func(t *testing.T) {
		itr := NewIterator(start, end, ascending, nil)
		performTest(t, itr)
		require.ErrorIs(t, errIteratorNilTreeGiven, itr.Error())
	})

	t.Run("Fast Iterator", func(t *testing.T) {
		itr := NewFastIterator(start, end, ascending, nil)
		performTest(t, itr)
		require.ErrorIs(t, errFastIteratorNilNdbGiven, itr.Error())
	})

	t.Run("Unsaved Fast Iterator", func(t *testing.T) {
		itr := NewUnsavedFastIterator(start, end, ascending, nil, map[string]*FastNode{}, map[string]interface{}{})
		performTest(t, itr)
		require.ErrorIs(t, errFastIteratorNilNdbGiven, itr.Error())
	})
}

func TestUnsavedFastIterator_NewIterator_NilAdditions_Failure(t *testing.T) {
	var start, end []byte = []byte{'a'}, []byte{'c'}
	ascending := true

	performTest := func(t *testing.T, itr dbm.Iterator) {
		require.NotNil(t, itr)
		require.False(t, itr.Valid())
		actualsStart, actualEnd := itr.Domain()
		require.Equal(t, start, actualsStart)
		require.Equal(t, end, actualEnd)
		require.Error(t, itr.Error())
	}

	t.Run("Nil additions given", func(t *testing.T) {
		tree, err := NewMutableTree(dbm.NewMemDB(), 0)
		require.NoError(t, err)
		itr := NewUnsavedFastIterator(start, end, ascending, tree.ndb, nil, tree.unsavedFastNodeRemovals)
		performTest(t, itr)
		require.ErrorIs(t, errUnsavedFastIteratorNilAdditionsGiven, itr.Error())
	})

	t.Run("Nil removals given", func(t *testing.T) {
		tree, err := NewMutableTree(dbm.NewMemDB(), 0)
		require.NoError(t, err)
		itr := NewUnsavedFastIterator(start, end, ascending, tree.ndb, tree.unsavedFastNodeAdditions, nil)
		performTest(t, itr)
		require.ErrorIs(t, errUnsavedFastIteratorNilRemovalsGiven, itr.Error())
	})

	t.Run("All nil", func(t *testing.T) {
		itr := NewUnsavedFastIterator(start, end, ascending, nil, nil, nil)
		performTest(t, itr)
		require.ErrorIs(t, errFastIteratorNilNdbGiven, itr.Error())
	})

	t.Run("Additions and removals are nil", func(t *testing.T) {
		tree, err := NewMutableTree(dbm.NewMemDB(), 0)
		require.NoError(t, err)
		itr := NewUnsavedFastIterator(start, end, ascending, tree.ndb, nil, nil)
		performTest(t, itr)
		require.ErrorIs(t, errUnsavedFastIteratorNilAdditionsGiven, itr.Error())
	})
}

func TestIterator_Empty_Invalid(t *testing.T) {
	config := &iteratorTestConfig{
		startByteToSet: 'a',
		endByteToSet:   'z',
		startIterate:   []byte("a"),
		endIterate:     []byte("a"),
		ascending:      true,
	}

	performTest := func(t *testing.T, itr dbm.Iterator, mirror [][]string) {
		require.Equal(t, 0, len(mirror))
		require.False(t, itr.Valid())
	}

	t.Run("Iterator", func(t *testing.T) {
		itr, mirror := setupIteratorAndMirror(t, config)
		performTest(t, itr, mirror)
	})

	t.Run("Fast Iterator", func(t *testing.T) {
		itr, mirror := setupFastIteratorAndMirror(t, config)
		performTest(t, itr, mirror)
	})

	t.Run("Unsaved Fast Iterator", func(t *testing.T) {
		itr, mirror := setupUnsavedFastIterator(t, config)
		performTest(t, itr, mirror)
	})
}

func TestIterator_Basic_Ranged_Ascending_Success(t *testing.T) {
	config := &iteratorTestConfig{
		startByteToSet: 'a',
		endByteToSet:   'z',
		startIterate:   []byte("e"),
		endIterate:     []byte("w"),
		ascending:      true,
	}

	performTest := func(t *testing.T, itr dbm.Iterator, mirror [][]string) {
		actualStart, actualEnd := itr.Domain()
		require.Equal(t, config.startIterate, actualStart)
		require.Equal(t, config.endIterate, actualEnd)

		require.NoError(t, itr.Error())

		assertIterator(t, itr, mirror, config.ascending)
	}

	t.Run("Iterator", func(t *testing.T) {
		itr, mirror := setupIteratorAndMirror(t, config)
		require.True(t, itr.Valid())
		performTest(t, itr, mirror)
	})

	t.Run("Fast Iterator", func(t *testing.T) {
		itr, mirror := setupFastIteratorAndMirror(t, config)
		require.True(t, itr.Valid())
		performTest(t, itr, mirror)
	})

	t.Run("Unsaved Fast Iterator", func(t *testing.T) {
		itr, mirror := setupUnsavedFastIterator(t, config)
		require.True(t, itr.Valid())
		performTest(t, itr, mirror)
	})
}

func TestIterator_Basic_Ranged_Descending_Success(t *testing.T) {
	config := &iteratorTestConfig{
		startByteToSet: 'a',
		endByteToSet:   'z',
		startIterate:   []byte("e"),
		endIterate:     []byte("w"),
		ascending:      false,
	}

	performTest := func(t *testing.T, itr dbm.Iterator, mirror [][]string) {
		actualStart, actualEnd := itr.Domain()
		require.Equal(t, config.startIterate, actualStart)
		require.Equal(t, config.endIterate, actualEnd)

		require.NoError(t, itr.Error())

		assertIterator(t, itr, mirror, config.ascending)
	}

	t.Run("Iterator", func(t *testing.T) {
		itr, mirror := setupIteratorAndMirror(t, config)
		require.True(t, itr.Valid())
		performTest(t, itr, mirror)
	})

	t.Run("Fast Iterator", func(t *testing.T) {
		itr, mirror := setupFastIteratorAndMirror(t, config)
		require.True(t, itr.Valid())
		performTest(t, itr, mirror)
	})

	t.Run("Unsaved Fast Iterator", func(t *testing.T) {
		itr, mirror := setupUnsavedFastIterator(t, config)
		require.True(t, itr.Valid())
		performTest(t, itr, mirror)
	})
}

func TestIterator_Basic_Full_Ascending_Success(t *testing.T) {
	config := &iteratorTestConfig{
		startByteToSet: 'a',
		endByteToSet:   'z',
		startIterate:   nil,
		endIterate:     nil,
		ascending:      true,
	}

	performTest := func(t *testing.T, itr dbm.Iterator, mirror [][]string) {
		actualStart, actualEnd := itr.Domain()
		require.Equal(t, config.startIterate, actualStart)
		require.Equal(t, config.endIterate, actualEnd)

		require.NoError(t, itr.Error())

		assertIterator(t, itr, mirror, config.ascending)
	}

	t.Run("Iterator", func(t *testing.T) {
		itr, mirror := setupIteratorAndMirror(t, config)
		require.True(t, itr.Valid())
		require.Equal(t, 25, len(mirror))
		performTest(t, itr, mirror)
	})

	t.Run("Fast Iterator", func(t *testing.T) {
		itr, mirror := setupFastIteratorAndMirror(t, config)
		require.True(t, itr.Valid())
		require.Equal(t, 25, len(mirror))
		performTest(t, itr, mirror)
	})

	t.Run("Unsaved Fast Iterator", func(t *testing.T) {
		itr, mirror := setupUnsavedFastIterator(t, config)
		require.True(t, itr.Valid())
		require.Equal(t, 25 - 25 / 4 + 1, len(mirror)) // to account for removals
		performTest(t, itr, mirror)
	})
}

func TestIterator_Basic_Full_Descending_Success(t *testing.T) {
	config := &iteratorTestConfig{
		startByteToSet: 'a',
		endByteToSet:   'z',
		startIterate:   nil,
		endIterate:     nil,
		ascending:      false,
	}

	performTest := func(t *testing.T, itr dbm.Iterator, mirror [][]string) {
		actualStart, actualEnd := itr.Domain()
		require.Equal(t, config.startIterate, actualStart)
		require.Equal(t, config.endIterate, actualEnd)

		require.NoError(t, itr.Error())

		assertIterator(t, itr, mirror, config.ascending)
	}

	t.Run("Iterator", func(t *testing.T) {
		itr, mirror := setupIteratorAndMirror(t, config)
		require.Equal(t, 25, len(mirror))
		require.True(t, itr.Valid())
		performTest(t, itr, mirror)
	})

	t.Run("Fast Iterator", func(t *testing.T) {
		itr, mirror := setupFastIteratorAndMirror(t, config)
		require.Equal(t, 25, len(mirror))
		require.True(t, itr.Valid())
		performTest(t, itr, mirror)
	})

	t.Run("Unsaved Fast Iterator", func(t *testing.T) {
		itr, mirror := setupUnsavedFastIterator(t, config)
		require.Equal(t, 25 - 25 / 4 + 1, len(mirror)) // to account for removals
		require.True(t, itr.Valid())
		performTest(t, itr, mirror)
	})
}

func TestIterator_WithDelete_Full_Ascending_Success(t *testing.T) {
	config := &iteratorTestConfig{
		startByteToSet: 'a',
		endByteToSet:   'z',
		startIterate:   nil,
		endIterate:     nil,
		ascending:      false,
	}

	tree, mirror := getRandomizedTreeAndMirror(t)

	_, _, err := tree.SaveVersion()
	require.NoError(t, err)

	randomizeTreeAndMirror(t, tree, mirror)

	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	err = tree.DeleteVersion(1)
	require.NoError(t, err)

	immutableTree, err := tree.GetImmutable(tree.ndb.getLatestVersion())
	require.NoError(t, err)

	// sort mirror for assertion
	sortedMirror := make([][]string, 0, len(mirror))
	for k, v := range mirror {
		sortedMirror = append(sortedMirror, []string{k, v})
	}

	sort.Slice(sortedMirror, func(i, j int) bool {
		return sortedMirror[i][0] > sortedMirror[j][0]
	})

	t.Run("Iterator", func(t *testing.T) {
		itr := NewIterator(config.startIterate, config.endIterate, config.ascending, immutableTree)
		require.True(t, itr.Valid())
		assertIterator(t, itr, sortedMirror, config.ascending)
	})

	t.Run("Fast Iterator", func(t *testing.T) {
		itr := NewFastIterator(config.startIterate, config.endIterate, config.ascending, immutableTree.ndb)
		require.True(t, itr.Valid())
		assertIterator(t, itr, sortedMirror, config.ascending)
	})

	t.Run("Unsaved Fast Iterator", func(t *testing.T) {
		itr := NewUnsavedFastIterator(config.startIterate, config.endIterate, config.ascending, immutableTree.ndb, tree.unsavedFastNodeAdditions, tree.unsavedFastNodeRemovals)
		require.True(t, itr.Valid())
		assertIterator(t, itr, sortedMirror, config.ascending)
	})
}

func setupIteratorAndMirror(t *testing.T, config *iteratorTestConfig) (dbm.Iterator, [][]string) {
	tree, err := NewMutableTree(dbm.NewMemDB(), 0)
	require.NoError(t, err)

	mirror := setupMirrorForIterator(t, config, tree)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	immutableTree, err := tree.GetImmutable(tree.ndb.getLatestVersion())
	require.NoError(t, err)

	itr := NewIterator(config.startIterate, config.endIterate, config.ascending, immutableTree)
	return itr, mirror
}

func setupFastIteratorAndMirror(t *testing.T, config *iteratorTestConfig) (dbm.Iterator, [][]string) {
	tree, err := NewMutableTree(dbm.NewMemDB(), 0)
	require.NoError(t, err)

	mirror := setupMirrorForIterator(t, config, tree)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	itr := NewFastIterator(config.startIterate, config.endIterate, config.ascending, tree.ndb)
	return itr, mirror
}

func setupUnsavedFastIterator(t *testing.T, config *iteratorTestConfig) (dbm.Iterator, [][]string) {
	tree, err := NewMutableTree(dbm.NewMemDB(), 0)
	require.NoError(t, err)

	// For unsaved fast iterator, we would like to test the state where
	// there are saved fast nodes as well as some unsaved additions and removals.
	// So, we split the byte range in half where the first half is saved and the second half is unsaved.
	breakpointByte := (config.endByteToSet + config.startByteToSet) / 2

	firstHalfConfig := *config
	firstHalfConfig.endByteToSet = breakpointByte // exclusive

	secondHalfConfig := *config
	secondHalfConfig.startByteToSet = breakpointByte

	firstHalfMirror := setupMirrorForIterator(t, &firstHalfConfig, tree)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	// No unsaved additions or removals should be present after saving
	require.Equal(t, 0, len(tree.unsavedFastNodeAdditions))
	require.Equal(t, 0, len(tree.unsavedFastNodeRemovals))

	// Ensure that there are unsaved additions and removals present
	secondHalfMirror := setupMirrorForIterator(t, &secondHalfConfig, tree)

	require.True(t, len(tree.unsavedFastNodeAdditions) >= len(secondHalfMirror))
	require.Equal(t, 0, len(tree.unsavedFastNodeRemovals))

	// Merge the two halves
	var mergedMirror [][]string
	if config.ascending {
		mergedMirror = append(firstHalfMirror, secondHalfMirror...)
	} else {
		mergedMirror = append(secondHalfMirror, firstHalfMirror...)
	}

	if len(mergedMirror) > 0 {
		// Remove random keys
		for i := 0; i < len(mergedMirror) / 4; i++ {
			randIndex := rand.Intn(len(mergedMirror))
			keyToRemove := mergedMirror[randIndex][0]

			_, removed := tree.Remove([]byte(keyToRemove))
			require.True(t, removed)

			mergedMirror = append(mergedMirror[:randIndex], mergedMirror[randIndex+1:]...)
		}
	}

	itr := NewUnsavedFastIterator(config.startIterate, config.endIterate, config.ascending, tree.ndb, tree.unsavedFastNodeAdditions, tree.unsavedFastNodeRemovals)
	return itr, mergedMirror
}
