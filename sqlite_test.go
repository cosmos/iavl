package iavl

import (
	"context"
	"fmt"
	"os"
	"testing"

	"cosmossdk.io/log"
	dbm "github.com/cosmos/cosmos-db"
	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func TestSqliteDb_SaveVersion(t *testing.T) {
	dir, err := os.MkdirTemp("", "testdb")
	fmt.Printf("dir: %s\n", dir)
	require.NoError(t, err)
	require.NoError(t, os.Remove(dir))

	sqlDb, err := NewSqliteDb(context.Background(), dir)
	require.NoError(t, err)

	levelDb, err := dbm.NewGoLevelDBWithOpts("testdb", dir, &opt.Options{})
	require.NoError(t, err)

	tree := NewMutableTreeWithOpts(levelDb, 0,
		//&Options{NodeBackend: sqlDb},
		//&Options{},
		&Options{NodeBackend: NewMapDB()},
		false, log.NewNopLogger())
	for i := 0; i < 1000; i++ {
		for j := 1000; j > 0; j-- {
			_, err := tree.Set([]byte(fmt.Sprintf("key%d", i+j)), []byte("value"))
			require.NoError(t, err)
		}
		_, v, err := tree.SaveVersion()

		require.NoError(t, err)
		require.Equal(t, int64(i+1), v)
	}

	fmt.Printf("getNodeCount %d, getNodeTime %d, GetNode: μ%2.2f\n",
		getNodeCount, getNodeTime, float64(getNodeTime)/float64(getNodeCount)/1000)

	require.NoError(t, sqlDb.Close())
}

func TestSqliteDb_GetNode(t *testing.T) {
	dir, err := os.MkdirTemp("", "testdb")
	t.Logf("dir: %s", dir)
	require.NoError(t, err)
	require.NoError(t, os.Remove(dir))

	db, err := NewSqliteDb(context.Background(), dir)
	require.NoError(t, err)

	err = db.storage.Exec(
		`INSERT INTO node(version, sequence, key, size, height, left_version, 
                 left_sequence, right_version, right_sequence) 
VALUES(1, 2, "key1", 5, 5, 1, 2, 1, 3)`)
	require.NoError(t, err)
	nk := &NodeKey{1, 2}
	n, err := db.GetNode(nk.GetKey())
	require.NoError(t, err)
	require.Equal(t, nk, n.nodeKey)
	require.Equal(t, "key1", string(n.key))

	require.NoError(t, db.Close())
	require.NoError(t, os.Remove(fmt.Sprintf("%s/%s", dir, "latest.db")))
	require.NoError(t, os.Remove(fmt.Sprintf("%s/%s", dir, "storage.db")))
	require.NoError(t, os.Remove(dir))

}
