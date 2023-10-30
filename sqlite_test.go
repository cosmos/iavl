package iavl

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/cosmos/iavl/v2/testutil"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

/*
Benchmarks measured from these leafRead-leafWrite tests below:

# SQLite

## Writes

no index:
- structured batch insert (node table) - 507,700 nodes/sec
- unstructured batch insert (tree table) re-use same bytes.Buffer - 444,000 nodes/sec
- unstructured batch insert (tree table) alloc new bytes.Buffer - 473,800 nodes/sec
  - !! surprising, why? because GC is async? probably worse in long tail run?

indexed:
- structured batch insert (node table) - 441,000 nodes/sec
  - the difference between indexed and not is not significant.  the only way I can explain this is that the writes
    below are sequential proceeding version number.  this will always be the case with the current node key.
- unstructured batch insert (tree table) - 414,000 nodes/sec

writing into a trie based table (indexed on key) will likely be *much* slower since it requires an order insertion
and potentially re-balancing the BTree index.
^^^ True, initial test started at 200k and quickly declined to 75k

## Reads

- fully memory mapped unstructured (tree table) - ~160,000 nodes/sec
- fully memory mapped structured (node table) - ~172,000 nodes/sec
- fully memory mapped structured (node table) leafRead by key []byte - ~160,000 nodes/sec

# LevelDB
Writes: 245,000 nodes/sec
Reads: 30,000 nodes/sec !!!

*/

var testDbLocation = "/tmp/sqlite_test"

func TestBuildSqlite(t *testing.T) {
	//dir := t.TempDir()
	dir := testDbLocation
	t.Logf("using temp dir %s", dir)

	sql, err := NewSqliteDb(NewNodePool(), SqliteDbOptions{Path: dir})

	require.NoError(t, err)

	gen := testutil.OsmoLike()
	version1 := gen.Iterator.Nodes()
	var count int
	require.Equal(t, int64(1), gen.Iterator.Version())

	since := time.Now()

	err = sql.leafWrite.Exec("CREATE TABLE node (seq INTEGER, version INTEGER, hash BLOB, key BLOB, height INTEGER, size INTEGER, l_seq INTEGER, l_version INTEGER, r_seq INTEGER, r_version INTEGER)")
	require.NoError(t, err)

	err = sql.leafWrite.Exec("CREATE INDEX trie_idx ON node (key)")
	//err = sql.leafWrite.Exec("CREATE INDEX node_idx ON node (version, seq)")
	require.NoError(t, err)
	err = sql.leafWrite.Exec("CREATE INDEX tree_idx ON tree (version, sequence)")
	require.NoError(t, err)

	require.NoError(t, sql.leafWrite.Begin())

	var stmt *sqlite3.Stmt
	//stmt, err = sql.leafWrite.Prepare("INSERT INTO tree(version, sequence, bytes) VALUES (?, ?, ?)")
	stmt, err = sql.leafWrite.Prepare("INSERT INTO node(version, seq, hash, key, height, size, l_seq, l_version, r_seq, r_version)" +
		"VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

	require.NoError(t, err)

	startTime := time.Now()
	batchSize := 200_000
	//nodeBz := new(bytes.Buffer)
	for ; version1.Valid(); err = version1.Next() {
		node := version1.GetNode()
		lnk := NewNodeKey(1, uint32(count+1))
		rnk := NewNodeKey(1, uint32(count+2))
		n := &Node{key: node.Key, hash: node.Key[:32],
			subtreeHeight: 13, size: 4, leftNodeKey: lnk, rightNodeKey: rnk}

		//nodeBz.Reset()
		//require.NoError(t, n.WriteBytes(nodeBz))

		// tree table
		//nk := NewNodeKey(1, uint32(count))
		//nodeBz, err := n.Bytes()
		//require.NoError(t, err)
		//err = stmt.Exec(int(nk.Version()), int(nk.Sequence()), nodeBz)
		//require.NoError(t, err)

		// node table
		err = stmt.Exec(
			1,          // version
			count,      // seq
			n.key[:32], // hash
			n.key,      // key
			13,         // height
			4,          // size
			count+1,    // l_seq
			1,          // l_version
			count+2,    // r_seq
			1,          // r_version
		)

		if count%batchSize == 0 {
			err := sql.leafWrite.Commit()
			require.NoError(t, err)
			//stmt, err = newBatch()
			//require.NoError(t, err)
			require.NoError(t, sql.leafWrite.Begin())
			log.Info().Msgf("nodes=%s dur=%s; rate=%s",
				humanize.Comma(int64(count)),
				time.Since(since).Round(time.Millisecond),
				humanize.Comma(int64(float64(batchSize)/time.Since(since).Seconds())))
			since = time.Now()
		}
		count++
		require.NoError(t, err)
	}

	log.Info().Msg("final commit")
	require.NoError(t, sql.leafWrite.Commit())
	log.Info().Msgf("total dur=%s rate=%s",
		time.Since(startTime).Round(time.Millisecond),
		humanize.Comma(int64(40_000_000/time.Since(startTime).Seconds())),
	)
	require.NoError(t, stmt.Close())
	require.NoError(t, sql.Close())
}

func TestReadSqlite_Trie(t *testing.T) {
	dir := testDbLocation
	sql, err := NewSqliteDb(NewNodePool(), SqliteDbOptions{Path: dir})
	require.NoError(t, err)

	read, err := sql.getReadConn()
	require.NoError(t, err)

	query, err := read.Prepare("SELECT version, seq, hash, key, height, size, l_seq, l_version, r_seq, r_version FROM node WHERE key = ?")
	require.NoError(t, err)

	var hash, key []byte
	var version, seq, height, size, lSeq, lVersion, rSeq, rVersion int

	i := int64(1)
	since := time.Now()
	gen := testutil.OsmoLike()
	version1 := gen.Iterator.Nodes()
	for ; version1.Valid(); err = version1.Next() {
		node := version1.GetNode()
		require.NoError(t, query.Bind(node.Key))
		hasRow, err := query.Step()
		require.NoError(t, err)
		require.True(t, hasRow)
		require.NoError(t, query.Scan(&version, &seq, &hash, &key, &height, &size, &lSeq, &lVersion, &rSeq, &rVersion))
		require.NoError(t, err)

		if i%100_000 == 0 {
			i++
			log.Info().Msgf("nodes=%s dur=%s; rate=%s",
				humanize.Comma(i),
				time.Since(since),
				humanize.Comma(int64(float64(100_000)/time.Since(since).Seconds())))
			since = time.Now()
		}
		require.NoError(t, query.Reset())
		i++
	}

}

func TestReadSqlite(t *testing.T) {
	//pool := NewNodePool()
	//dir := t.TempDir()
	var err error
	dir := testDbLocation
	t.Logf("using temp dir %s", dir)
	sql, err := NewSqliteDb(NewNodePool(), SqliteDbOptions{Path: dir})
	require.NoError(t, err)

	var stmt *sqlite3.Stmt
	//stmt, err = sql.leafWrite.Prepare("SELECT bytes FROM tree WHERE node_key = ?")

	sqlRead, err := sql.getReadConn()
	require.NoError(t, err)
	//stmt, err = sqlRead.Prepare("SELECT bytes FROM tree WHERE version = ? AND sequence = ?")
	stmt, err = sqlRead.Prepare("SELECT hash, key, height, size, l_seq, l_version, r_seq, r_version FROM node WHERE seq = ? AND version = ?")
	require.NoError(t, err)

	var hash, key []byte
	var height, size, lSeq, lVersion, rSeq, rVersion int

	since := time.Now()
	for i := 1; i < 40_000_000; i++ {
		j := rand.Intn(40_000_000)

		// unstructured leafRead:
		//nk := NewNodeKey(1, uint32(j))
		//require.NoError(t, stmt.Bind(1, j))
		//hasRow, err := stmt.Step()
		//require.Truef(t, hasRow, "no row for %d", j)
		//require.NoError(t, err)
		//nodeBz, err := stmt.ColumnBlob(0)
		//require.NoError(t, err)
		//_, err = MakeNode(pool, nk, nodeBz)
		//require.NoError(t, err)

		// structured leafRead:
		require.NoError(t, stmt.Bind(j, 1))
		hasRow, err := stmt.Step()
		require.NoError(t, err)
		require.True(t, hasRow)
		require.NoError(t, stmt.Scan(&hash, &key, &height, &size, &lSeq, &lVersion, &rSeq, &rVersion))

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

func TestNodeKeyFormat(t *testing.T) {
	nk := NewNodeKey(100, 2)
	k := (int(nk.Version()) << 32) | int(nk.Sequence())
	fmt.Printf("k: %d - %x\n", k, k)
}

func TestFetchNode(t *testing.T) {
	pool := NewNodePool()
	conn, err := sqlite3.Open("/tmp/iavl-v2.db")
	require.NoError(t, err)
	q := "SELECT bytes FROM tree_1 WHERE version = 1 and sequence = 6756148"
	stmt, err := conn.Prepare(q)
	require.NoError(t, err)
	hasRow, err := stmt.Step()
	require.NoError(t, err)
	require.True(t, hasRow)
	nodeBz, err := stmt.ColumnBlob(0)
	require.NoError(t, err)
	nk := NewNodeKey(1, 6756148)
	node, err := MakeNode(pool, nk, nodeBz)
	require.NoError(t, err)
	fmt.Printf("node: %v\n", node)
}

func TestMmap(t *testing.T) {
	tmpDir := t.TempDir()
	conn, err := sqlite3.Open(tmpDir + "/test.db")
	require.NoError(t, err)
	stmt, err := conn.Prepare("PRAGMA mmap_size=1000000000000")
	require.NoError(t, err)
	ok, err := stmt.Step()
	require.NoError(t, err)
	require.True(t, ok)

	stmt, err = conn.Prepare("PRAGMA mmap_size")
	require.NoError(t, err)
	ok, err = stmt.Step()
	require.NoError(t, err)
	require.True(t, ok)
	res, ok, err := stmt.ColumnRawString(0)
	require.True(t, ok)
	require.NoError(t, err)
	fmt.Printf("res: %s\n", res)
}

func Test_NewSqliteDb(t *testing.T) {
	dir := t.TempDir()
	sql, err := NewSqliteDb(NewNodePool(), SqliteDbOptions{Path: dir})
	require.NoError(t, err)
	require.NotNil(t, sql)
}
