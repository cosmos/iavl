package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"

	amino "github.com/tendermint/go-amino"
	"github.com/tendermint/iavl"
	dbm "github.com/tendermint/tm-db"
)

// TODO: make this configurable?
const (
	DefaultCacheSize int = 10000
)

// NOTE: this is hacked to work with cosmos-sdk.RootMultiStore

func main() {
	args := os.Args[1:]
	if len(args) < 2 || (args[0] != "data" && args[0] != "shape" && args[0] != "versions" && args[0] != "stores" && args[0] != "rollback") {
		fmt.Fprintln(os.Stderr, "Usage: iaviewer <data|shape|versions|stores|rollback> <leveldb dir> [store] [version number]")
		os.Exit(1)
	}

	store := ""
	if len(args) > 2 {
		store = args[2]
	}

	version := 0
	if len(args) == 4 {
		var err error
		version, err = strconv.Atoi(args[3])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid version number: %s\n", err)
			os.Exit(1)
		}
	}

	db, err := OpenDb(args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot open %s: %s\n", args[1], err)
		os.Exit(1)
	}

	latest, err := GetLatestVersion(db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot show stores: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Latest: %d\n", latest)

	if args[0] == "rollback" {
		// TODO: rollback all substores
		err = SetLatestVersion(db, latest-2)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Cannot show stores: %s\n", err)
			os.Exit(1)
		}
	}

	if version == 0 {
		version = int(latest)
	}

	if args[0] == "stores" {
		stores, err := GetSubStores(db, latest)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Cannot show stores: %s\n", err)
			os.Exit(1)
		}

		for _, store := range stores {
			fmt.Println(store)
		}
		return
	}

	tree, err := ReadTree(db, store, version)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading tree: %s\n", err)
		os.Exit(1)
	}

	if args[0] == "data" {
		PrintKeys(tree)
		fmt.Printf("Hash: %X\n", tree.Hash())
		fmt.Printf("Size: %X\n", tree.Size())
	} else if args[0] == "shape" {
		PrintShape(tree)
	} else if args[0] == "versions" {
		PrintVersions(tree)
	}
}

func OpenDb(dir string) (dbm.DB, error) {
	if strings.HasSuffix(dir, ".db") {
		dir = dir[:len(dir)-3]
	} else if strings.HasSuffix(dir, ".db/") {
		dir = dir[:len(dir)-4]
	} else {
		return nil, fmt.Errorf("database directory must end with .db")
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

func PrintDbStats(db dbm.DB) {
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

type commitInfo struct {
	// Version
	Version int64
	// Store info for
	StoreInfos []storeInfo
}

type storeInfo struct {
	Name string
	Core storeCore
}

type storeCore struct {
	CommitID CommitID
}

type CommitID struct {
	Version int64
	Hash    []byte
}

func SetLatestVersion(db dbm.DB, latest int64) error {
	cdc := amino.NewCodec()
	bz, err := cdc.MarshalBinaryLengthPrefixed(latest)
	if err != nil {
		return err
	}
	db.Set([]byte("s/latest"), bz)
	return nil
}

func GetLatestVersion(db dbm.DB) (int64, error) {
	cdc := amino.NewCodec()
	bz := db.Get([]byte("s/latest"))
	var latest int64
	err := cdc.UnmarshalBinaryLengthPrefixed(bz, &latest)
	return latest, err
}

func GetSubStores(db dbm.DB, ver int64) ([]string, error) {
	var cInfo commitInfo
	cdc := amino.NewCodec()

	stateKey := []byte(fmt.Sprintf("s/%d", ver))
	stateBz := db.Get(stateKey)
	err := cdc.UnmarshalBinaryLengthPrefixed(stateBz, &cInfo)
	if err != nil {
		return nil, err
	}

	var res []string
	for _, sinfo := range cInfo.StoreInfos {
		res = append(res, sinfo.Name)
	}
	return res, nil
}

// ReadTree loads an iavl tree from the directory
// If version is 0, load latest, otherwise, load named version
func ReadTree(db dbm.DB, store string, version int) (*iavl.MutableTree, error) {
	key := "s/k:" + store + "/"
	subdb := dbm.NewPrefixDB(db, []byte(key))
	tree := iavl.NewMutableTree(subdb, DefaultCacheSize)

	ver, err := tree.LoadVersion(int64(version))
	fmt.Printf("Want Version: %d\nGot version: %d\n", version, ver)
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

func PrintShape(tree *iavl.MutableTree) {
	// shape := tree.RenderShape("  ", nil)
	shape := tree.RenderShape("  ", nodeEncoder)
	fmt.Println(strings.Join(shape, "\n"))
}

func nodeEncoder(id []byte, depth int, isLeaf bool) string {
	prefix := fmt.Sprintf("-%d ", depth)
	if isLeaf {
		prefix = fmt.Sprintf("*%d ", depth)
	}
	if len(id) == 0 {
		return fmt.Sprintf("%s<nil>", prefix)
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
