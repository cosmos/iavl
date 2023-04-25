package main

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/cosmos/iavl"
)

const (
	DefaultCacheSize = 1000
)

func main() {
	args := os.Args[1:]
	if len(args) < 4 {
		fmt.Fprintln(os.Stderr, "Usage: dbgenerator <dbtype> <dbdir> <random|sequential> <version>")
		os.Exit(1)
	}

	version, err := strconv.Atoi(args[3])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid version number: %s\n", err)
		os.Exit(1)
	}

	if err = generateTree(args[0], args[1], args[2], version); err != nil {
		fmt.Fprintf(os.Stderr, "Error generating tree: %s\n", err)
	}
}

func openDB(dbType, dbDir string) (dbm.DB, error) {
	dir, err := filepath.Abs(dbDir)
	if err != nil {
		return nil, err
	}

	db, err := dbm.NewDB("test", dbm.BackendType(dbType), dir)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func generateTree(dbType, dbDir, mode string, version int) error {
	db, err := openDB(dbType, dbDir)
	if err != nil {
		return err
	}
	defer db.Close()

	switch mode {
	case "random":
		return generateRandomTree(db, version)
	case "sequential":
		_, err = generateSequentialTree(db, version)
		return err
	default:
		return fmt.Errorf("invalid mode: %s", mode)
	}
}

func generateRandomTree(db dbm.DB, version int) error {
	t, err := generateSequentialTree(db, version)
	if err != nil {
		return err
	}

	// delete the half of the versions
	versions := make([]int64, version)
	for i := 0; i < version; i++ {
		versions[i] = int64(i + 1)
	}

	// make sure the latest version is not deleted
	for i := 1; i <= version/2; i++ {
		index := rand.Intn(version - i)
		if err := t.DeleteVersion(versions[index]); err != nil {
			return err
		}
		versions[index], versions[version-i-1] = versions[version-i-1], versions[index]
	}

	return nil
}

func generateSequentialTree(db dbm.DB, version int) (*iavl.MutableTree, error) {
	t, err := iavl.NewMutableTreeWithOpts(db, DefaultCacheSize, nil, false)
	if err != nil {
		return nil, err
	}

	for i := 0; i < version; i++ {
		leafCount := rand.Int31n(50)
		for j := int32(0); j < leafCount; j++ {
			t.Set(randBytes(32), randBytes(32))
		}
		if _, _, err = t.SaveVersion(); err != nil {
			return nil, err
		}
	}

	return t, err
}

func randBytes(length int) []byte {
	key := make([]byte, length)
	// math.rand.Read always returns err=nil
	// we do not need cryptographic randomness for this test:
	rand.Read(key)
	return key
}
