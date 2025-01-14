package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	corestore "cosmossdk.io/core/store"
	"cosmossdk.io/log"

	"github.com/cosmos/iavl"
	dbm "github.com/cosmos/iavl/db"
)

// TODO: make this configurable?
const (
	DefaultCacheSize int = 10000
)

var cmds = map[string]bool{
	"data":      true,
	"data-full": true,
	"hash":      true,
	"shape":     true,
	"versions":  true,
}

func main() {
	args := os.Args[1:]
	if len(args) < 3 || len(args) > 4 || !cmds[args[0]] {
		fmt.Fprintln(os.Stderr, strings.TrimSpace(`
Usage: iaviewer <data|data-full|hash|shape|versions> <leveldb dir> <prefix> [version number]
<prefix> is the prefix of db, and the iavl tree of different modules in cosmos-sdk uses
different <prefix> to identify, just like "s/k:gov/" represents the prefix of gov module
`))
		os.Exit(1)
	}

	version := int64(0)
	if len(args) >= 4 {
		var err error
		version, err = strconv.ParseInt(args[3], 10, 0)
		assertNoError(err, "Invalid version number")
	}

	mutableTree, latestVersion, err := ReadTree(args[1], []byte(args[2]))
	assertNoError(err, "Error reading database")

	if args[0] == "versions" {
		PrintVersions(mutableTree)
		return
	}

	if version == 0 {
		version = latestVersion
	}
	tree, err := mutableTree.GetImmutable(version)
	assertNoError(err, "Error reading target version")
	fmt.Printf("Got version: %d\n", tree.Version())

	fullValues := false
	switch args[0] {
	case "data-full":
		fullValues = true
		fallthrough
	case "data":
		PrintKeys(tree, fullValues)
		fallthrough
	case "hash":
		hash := tree.Hash()
		fmt.Printf("Hash: %X\n", hash)
		fmt.Printf("Size: %X\n", tree.Size())
	case "shape":
		PrintShape(tree)
	}
}

func assertNoError(err error, msg string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", msg, err)
		os.Exit(1)
	}
}

func OpenDB(dir string) (corestore.KVStoreWithBatch, error) {
	switch {
	case strings.HasSuffix(dir, ".db"):
		dir = dir[:len(dir)-3]
	case strings.HasSuffix(dir, ".db/"):
		dir = dir[:len(dir)-4]
	default:
		return nil, errors.New("database directory must end with .db")
	}

	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}

	// TODO: doesn't work on windows!
	cut := strings.LastIndex(dir, "/")
	if cut == -1 {
		return nil, fmt.Errorf("cannot cut paths on %s", dir)
	}
	name := dir[cut+1:]
	db, err := dbm.NewGoLevelDB(name, dir[:cut])
	if err != nil {
		return nil, err
	}
	return db, nil
}

func PrintDBStats(db corestore.KVStoreWithBatch) {
	count := 0
	prefix := map[string]int{}
	itr, err := db.Iterator(nil, nil)
	if err != nil {
		panic(err)
	}

	defer itr.Close()
	for ; itr.Valid(); itr.Next() {
		key := itr.Key()[:1]
		prefix[string(key)]++
		count++
	}
	if err := itr.Error(); err != nil {
		panic(err)
	}
	fmt.Printf("DB contains %d entries\n", count)
	for k, v := range prefix {
		fmt.Printf("  %s: %d\n", k, v)
	}
}

// ReadTree loads an iavl tree from disk, applying the specified prefix if non-empty.
func ReadTree(dir string, prefix []byte) (tree *iavl.MutableTree, latestVersion int64, err error) {
	db, err := OpenDB(dir)
	if err != nil {
		return nil, 0, err
	}
	if len(prefix) != 0 {
		db = dbm.NewPrefixDB(db, prefix)
	}

	tree = iavl.NewMutableTree(db, DefaultCacheSize, true, log.NewLogger(os.Stdout))
	latestVersion, err = tree.Load()
	if err != nil {
		return nil, 0, err
	} else if tree.IsEmpty() {
		return nil, 0, fmt.Errorf("tree is empty")
	}
	fmt.Printf("Got latest version: %d\n", latestVersion)
	return tree, latestVersion, err
}

func PrintKeys(tree *iavl.ImmutableTree, fullValues bool) {
	valuesLabel := "hashed values"
	valueToString := func(value []byte) string {
		return fmt.Sprintf("%X", sha256.Sum256(value))
	}
	if fullValues {
		valuesLabel = "values"
		valueToString = encodeData
	}
	fmt.Printf("Printing all keys with %s (to detect diff)\n", valuesLabel)
	tree.Iterate(func(key []byte, value []byte) bool {
		keyStr := parseWeaveKey(key)
		valueStr := valueToString(value)
		fmt.Printf("  %s\n    %s\n", keyStr, valueStr)
		return false
	})
}

// parseWeaveKey returns a string representation of a key, splitting on the first ":"
// into a prefix (presumably an all-ASCII type label) followed by a possibly-binary suffix.
func parseWeaveKey(key []byte) string {
	cut := bytes.IndexRune(key, ':')
	if cut == -1 {
		return encodeData(key)
	}
	prefix := key[:cut]
	id := key[cut+1:]
	return fmt.Sprintf("%s:%s", encodeData(prefix), encodeData(id))
}

// encodeData returns a printable ASCII representation of its input;
// hexadecimal if it is not already printable ASCII, otherwise plain
// or quoted as necessary to avoid misinterpretation.
func encodeData(src []byte) string {
	hexConfusable := true
	forceQuotes := false
	for _, b := range src {
		if b < 0x20 || b >= 0x80 {
			return fmt.Sprintf("%X", src)
		}
		if b < '0' || (b > '9' && b < 'A') || (b > 'F') {
			hexConfusable = false
			if b == ' ' || b == '"' || b == '\\' {
				forceQuotes = true
			}
		}
	}
	if hexConfusable || forceQuotes {
		return fmt.Sprintf("%q", src)
	}
	return string(src)
}

func PrintShape(tree *iavl.ImmutableTree) {
	// shape := tree.RenderShape("  ", nil)
	// TODO: handle this error
	shape, _ := tree.RenderShape("  ", nodeEncoder)
	fmt.Println(strings.Join(shape, "\n"))
}

func nodeEncoder(id []byte, depth int, isLeaf bool) string {
	prefix := fmt.Sprintf("-%d ", depth)
	if isLeaf {
		prefix = fmt.Sprintf("*%d ", depth)
	}
	return fmt.Sprintf("%s%s", prefix, parseWeaveKey(id))
}

func PrintVersions(tree *iavl.MutableTree) {
	versions := tree.AvailableVersions()
	fmt.Println("Available versions:")
	for _, v := range versions {
		fmt.Printf("  %d\n", v)
	}
}
