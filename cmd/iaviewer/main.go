package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
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
	if len(args) != 2 || (args[0] != "data" && args[0] != "shape") {
		fmt.Println("Usage: iaviewer [data|shape] <leveldb dir>")
		os.Exit(1)
	}

	tree, err := ReadTree(args[1])
	if err != nil {
		fmt.Printf("Error reading data: %s\n", err)
		os.Exit(2)
	}
	fmt.Printf("Successfully read tree in %s\n", args[0])

	if args[0] == "data" {
		PrintKeys(tree)
		fmt.Printf("Hash: %X\n", tree.Hash())
		fmt.Printf("Size: %X\n", tree.Size())
	} else if args[0] == "shape" {
		PrintShape(tree)
	}
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
	fmt.Println("Printing all keys with hashed values (to detect diff)")
	tree.Iterate(func(key []byte, value []byte) bool {
		printKey := parseWeaveKey(key)
		digest := sha256.Sum256(value)
		fmt.Printf("  %s\n    %X\n", printKey, digest)
		return false
	})
}

// parseWeaveKey assumes a separating : where all in front should be ascii,
// and all afterwards may be ascii or binary
func parseWeaveKey(key []byte) string {
	cut := bytes.IndexRune(key, ':')
	if cut == -1 {
		return encodeId(key)
	}
	prefix := key[:cut]
	id := key[cut+1:]
	return fmt.Sprintf("%s:%s", encodeId(prefix), encodeId(id))
}

// casts to a string if it is printable ascii, hex-encodes otherwise
func encodeId(id []byte) string {
	for _, b := range id {
		if b < 0x20 || b >= 0x80 {
			return strings.ToUpper(hex.EncodeToString(id))
		}
	}
	return string(id)
}

func PrintShape(tree *iavl.MutableTree) error {
	_, _, proof, err := tree.GetRangeWithProof(nil, nil, 0)
	if err != nil {
		return err
	}

	fmt.Printf("Left path: %s\n", proof.LeftPath)
	for i, p := range proof.InnerNodes {
		fmt.Printf("%d: %s\n", i, p)
	}
	return nil
}
