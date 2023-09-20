package iavl

import (
	"math/rand"
	"testing"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/cosmos/iavl/v2/leveldb"
	"github.com/cosmos/iavl/v2/testutil"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

var testTreeTable = true

func TestBuildSqlite(t *testing.T) {
	dir := "/tmp"
	//dir := t.TempDir()
	t.Logf("using temp dir %s", dir)
	sql, err := newSqliteDb(dir, true)

	require.NoError(t, err)

	gen := testutil.OsmoLike()
	version1 := gen.Iterator.Nodes()
	var count int
	require.Equal(t, int64(1), gen.Iterator.Version())

	since := time.Now()
	newBatch := func() (*sqlite3.Stmt, error) {
		var err error
		err = sql.write.Begin()
		if err != nil {
			return nil, err
		}

		var stmt *sqlite3.Stmt
		if testTreeTable {
			stmt, err = sql.write.Prepare("INSERT INTO tree(version, sequence, bytes) VALUES (?, ?, ?)")
		} else {
			stmt, err = sql.write.Prepare("INSERT INTO node(seq, version, hash, key, height, size, l_seq, l_version, r_seq, r_version) " +
				"VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		}

		if err != nil {
			return nil, err
		}
		return stmt, nil
	}

	err = sql.write.Exec("CREATE INDEX IF NOT EXISTS tree_idx ON tree (version, sequence)")
	require.NoError(t, err)

	stmt, err := newBatch()
	require.NoError(t, err)

	batchSize := 200_000
	for ; version1.Valid(); err = version1.Next() {
		node := version1.GetNode()
		if testTreeTable {
			nk := &NodeKey{version: 1, sequence: uint32(count)}
			lnk := &NodeKey{version: 1, sequence: uint32(count + 1)}
			rnk := &NodeKey{version: 1, sequence: uint32(count + 2)}
			node := &Node{key: node.Key, hash: node.Key[:32], subtreeHeight: 13, size: 4,
				leftNodeKey: lnk.GetKey(), rightNodeKey: rnk.GetKey()}
			nodeBz, err := node.Bytes()
			require.NoError(t, err)
			err = stmt.Exec(nk.version, int(nk.sequence), nodeBz)
			require.NoError(t, err)
		} else {
			err = stmt.Exec(
				count,         // seq
				1,             // version
				node.Key[:32], // hash
				node.Key,      // key
				13,            // height
				4,             // size
				count+1,       // l_seq
				1,             // l_version
				count+2,       // r_seq
				1,             // r_version
			)
		}

		if count%batchSize == 0 {
			err := sql.write.Commit()
			require.NoError(t, err)
			stmt, err = newBatch()
			require.NoError(t, err)
			log.Info().Msgf("nodes=%s dur=%s; rate=%s",
				humanize.Comma(int64(count)),
				time.Since(since),
				humanize.Comma(int64(float64(batchSize)/time.Since(since).Seconds())))
			since = time.Now()
		}
		count++
		require.NoError(t, err)
	}

	log.Info().Msg("final commit")
	require.NoError(t, sql.write.Commit())
}

func TestReadSqlite(t *testing.T) {
	pool := newNodePool()
	//dir := t.TempDir()
	var err error
	dir := "/tmp"
	t.Logf("using temp dir %s", dir)
	sql, err := newSqliteDb(dir, false)
	require.NoError(t, err)

	var stmt *sqlite3.Stmt
	if testTreeTable {
		stmt, err = sql.write.Prepare("SELECT bytes FROM tree WHERE node_key = ?")
	} else {
		stmt, err = sql.write.Prepare("SELECT * FROM node WHERE seq = ? AND version = ?")
	}

	var hash, key []byte
	var height, size, lSeq, lVersion, rSeq, rVersion int
	since := time.Now()
	for i := 1; i < 80_000_000; i++ {
		j := rand.Intn(80_000_000)

		if testTreeTable {
			nk := &NodeKey{version: 1, sequence: uint32(j)}
			nkbz := nk.GetKey()
			require.NoError(t, stmt.Bind(nkbz))
			hasRow, err := stmt.Step()
			require.True(t, hasRow)
			require.NoError(t, err)
			nodeBz, err := stmt.ColumnBlob(0)
			require.NoError(t, err)
			_, err = MakeNode(pool, nkbz, nodeBz)
			require.NoError(t, err)
		} else {
			require.NoError(t, stmt.Bind(j, 1))
			hasRow, err := stmt.Step()
			require.NoError(t, err)
			require.True(t, hasRow)
			require.NoError(t, stmt.Scan(&hash, &key, &height, &size, &lSeq, &lVersion, &rSeq, &rVersion))
		}

		if i%100_000 == 0 {
			log.Info().Msgf("nodes=%s dur=%s; rate=%s",
				humanize.Comma(int64(i)),
				time.Since(since),
				humanize.Comma(int64(float64(100_000)/time.Since(since).Seconds())))
			since = time.Now()
		}
		require.NoError(t, stmt.Reset())
	}

	//gen := testutil.OsmoLike()
	//version1 := gen.Iterator.Nodes()
	//var count int
	//require.Equal(t, int64(1), gen.Iterator.Version())
}

func TestBuildLevelDb(t *testing.T) {
	//dir := t.TempDir()
	dir := "/tmp"
	t.Logf("using temp dir %s", dir)
	levelDb, err := leveldb.New("iavl_test", dir)
	require.NoError(t, err)
	db := &kvDB{db: levelDb}

	gen := testutil.OsmoLike()
	version1 := gen.Iterator.Nodes()
	var count int
	require.Equal(t, int64(1), gen.Iterator.Version())

	since := time.Now()
	for ; version1.Valid(); err = version1.Next() {
		node := version1.GetNode()
		nk := &NodeKey{version: 1, sequence: uint32(count)}
		lnk := &NodeKey{version: 1, sequence: uint32(count + 1)}
		rnk := &NodeKey{version: 1, sequence: uint32(count + 2)}
		n := &Node{key: node.Key,
			nodeKey:       nk,
			hash:          node.Key[:32],
			subtreeHeight: 13, size: 4,
			leftNodeKey:  lnk.GetKey(),
			rightNodeKey: rnk.GetKey(),
		}
		_, err = db.Set(n)
		require.NoError(t, err)

		if count%100_000 == 0 {
			log.Info().Msgf("nodes=%s dur=%s; rate=%s",
				humanize.Comma(int64(count)),
				time.Since(since),
				humanize.Comma(int64(float64(100_000)/time.Since(since).Seconds())))
			since = time.Now()
		}
		count++
	}
}

func TestReadLevelDB(t *testing.T) {
	dir := "/tmp"
	t.Logf("using temp dir %s", dir)
	levelDb, err := leveldb.New("iavl_test", dir)
	require.NoError(t, err)
	db := &kvDB{db: levelDb, pool: newNodePool()}

	since := time.Now()
	for i := 1; i < 80_000_000; i++ {
		j := rand.Intn(80_000_000)
		nk := &NodeKey{version: 1, sequence: uint32(j)}
		node, err := db.Get(nk)
		require.NoError(t, err)
		require.NotNil(t, node)
		if i%100_000 == 0 {
			log.Info().Msgf("nodes=%s dur=%s; rate=%s",
				humanize.Comma(int64(i)),
				time.Since(since),
				humanize.Comma(int64(float64(100_000)/time.Since(since).Seconds())))
			since = time.Now()
		}
	}
}
