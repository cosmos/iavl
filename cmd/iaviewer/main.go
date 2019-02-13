package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/tendermint/iavl"
	dbm "github.com/tendermint/tendermint/libs/db"
)

// TODO: make these configurable?
const (
	DefaultCacheSize int = 10000
)

func main() {
	args := os.Args[1:]
	if len(args) != 1 {
		fmt.Println("Usage: iaviewer <leveldb dir>")
		os.Exit(1)
	}
	tree, err := ReadTree(args[0])
	if err != nil {
		fmt.Printf("Error reading data: %s\n", err)
		os.Exit(2)
	}
	fmt.Printf("Successfully read tree in %s\n", args[0])
	fmt.Printf("  Hash: %X\n", tree.Hash())
	PrintKeys(tree)
}

func OpenDb(dir string) (dbm.DB, error) {
	if strings.HasSuffix(dir, ".db") {
		dir = dir[:len(dir)-3]
	} else if strings.HasSuffix(dir, ".db/") {
		dir = dir[:len(dir)-4]
	} else {
		return nil, fmt.Errorf("Database directory must end with .db")
	}
	// TODO: doesn't work on windows!
	cut := strings.LastIndex(dir, "/")
	if cut == -1 {
		return nil, fmt.Errorf("Cannot cut paths on %s", dir)
	}
	name := dir[cut+1:]
	fmt.Println(dir[:cut], name)
	db, err := dbm.NewGoLevelDB(name, dir[:cut])
	if err != nil {
		return nil, err
	}
	PrintDbStats(db)
	return db, nil
}

func PrintDbStats(db dbm.DB) {
	// stats, _ := json.MarshalIndent(db.Stats(), "", "  ")
	// fmt.Println(string(stats))

	count := 0
	prefix := map[string]int{}
	iter := db.Iterator(nil, nil)
	for ; iter.Valid(); iter.Next() {
		key := string(iter.Key()[:1])
		prefix[key] = prefix[key] + 1
		count++
	}
	iter.Close()
	fmt.Printf("DB contains %d entries\n", count)
	for k, v := range prefix {
		fmt.Printf("  %s: %d\n", k, v)
	}
}

func ReadTree(dir string) (*iavl.MutableTree, error) {
	db, err := OpenDb(dir)
	if err != nil {
		return nil, err
	}
	tree := iavl.NewMutableTree(db, DefaultCacheSize)
	ver, err := tree.Load()
	fmt.Printf("Got version: %d\n", ver)
	return tree, err
}

func PrintKeys(tree *iavl.MutableTree) {
	fmt.Println("Printing all keys")
}
