package iavl

// Generate013Orphans generates a GoLevelDB orphan database in testdata/0.13-orphans.db
// for testing Repair013Orphans(). It must be run with IAVL 0.13.x.
/*func TestGenerate013Orphans(t *testing.T) {
	err := os.RemoveAll("testdata/0.13-orphans.db")
	require.NoError(t, err)
	db, err := dbm.NewGoLevelDB("0.13-orphans", "testdata")
	require.NoError(t, err)
	tree, err := NewMutableTreeWithOpts(db, dbm.NewMemDB(), 0, &Options{
		KeepEvery:  3,
		KeepRecent: 1,
		Sync:       true,
	})
	require.NoError(t, err)
	version, err := tree.Load()
	require.NoError(t, err)
	require.EqualValues(t, 0, version)

	// We generate 8 versions. In each version, we create a "addX" key, delete a "rmX" key,
	// and update the "current" key, where "X" is the current version. Values are the version in
	// which the key was last set.
	tree.Set([]byte("rm1"), []byte{1})
	tree.Set([]byte("rm2"), []byte{1})
	tree.Set([]byte("rm3"), []byte{1})
	tree.Set([]byte("rm4"), []byte{1})
	tree.Set([]byte("rm5"), []byte{1})
	tree.Set([]byte("rm6"), []byte{1})
	tree.Set([]byte("rm7"), []byte{1})
	tree.Set([]byte("rm8"), []byte{1})

	for v := byte(1); v <= 8; v++ {
		tree.Set([]byte("current"), []byte{v})
		tree.Set([]byte(fmt.Sprintf("add%v", v)), []byte{v})
		tree.Remove([]byte(fmt.Sprintf("rm%v", v)))
		_, version, err = tree.SaveVersion()
		require.NoError(t, err)
		require.EqualValues(t, v, version)
	}

	// At this point, the database will contain incorrect orphans in version 6 that, when
	// version 6 is deleted, will cause "current", "rm7", and "rm8" to go missing.
}*/
