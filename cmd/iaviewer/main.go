package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"cosmossdk.io/log"
	dbm "github.com/cosmos/cosmos-db"

	"github.com/cosmos/iavl"
)

// TODO: make this configurable?
const (
	DefaultCacheSize int = 10000
)

func main() {
	args := os.Args[1:]

	// If version is the default, we will load the latest version of the tree.
	version := 0
	if len(args) >= 4 {
		var err error
		version, err = strconv.Atoi(args[3])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid version number: %s\n", err)
			return
		}
	}


	command := ""
	if len(args) > 0 {
		command = args[0]
	}

	switch command {
	case "data":
		tree, err := CreateAndLoadTree(args[1], version, []byte(args[2]))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating tree: %s\n", err)
			return
		}

		PrintKeys(tree)
		hash := tree.Hash()
		fmt.Printf("Hash: %X\n", hash)
		fmt.Printf("Size: %X\n", tree.Size())
	case "shape":
		tree, err := CreateAndLoadTree(args[1], version, []byte(args[2]))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating tree: %s\n", err)
			return
		}

		PrintShape(tree)
	case "versions":
		tree, err := CreateAndLoadTree(args[1], version, []byte(args[2]))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating tree: %s\n", err)
			return
		}

		fmt.Println("Available versions:", tree.AvailableVersions())
	case "tree-hash":
		dir, prefix := args[1], args[2]
		tree, err := CreateAndLoadTree(dir, version, []byte(prefix))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating tree: %s\n", err)
			return
		}

		fmt.Printf("Hash: %X\n", tree.Hash())
		fmt.Printf("Size: %X\n", tree.Size())
	case "delete-to":
		dir, prefix := args[1], args[2]
		targetVersion := 0
		if len(args) == 5 {
			var err error
			targetVersion, err = strconv.Atoi(args[4])
			if err != nil {
				fmt.Fprintf(os.Stderr, "Invalid delete version number: %s\n", err)
				return
			}
		}

		tree, err := CreateAndLoadTree(dir, version, []byte(prefix))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating tree: %s\n", err)
			return
		}

		fmt.Println("DeleteVersionsTo", int64(targetVersion))
		if err := tree.DeleteVersionsTo(int64(targetVersion)); err != nil {
			fmt.Fprintf(os.Stderr, "Error delete version: %s\n", err)
		}
	case "delete-tree":
		dir, prefix := args[1], args[2]

		db, err := OpenGoLevelDB(dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading data: %s\n", err)
			return
		}

		deleteKeysWithPrefix(prefix, db)
	case "export":
		dir, prefix := args[1], args[2]
		tree, err := CreateAndLoadTree(dir, version, []byte(prefix))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating tree: %s\n", err)
			return
		}
		fmt.Printf("Hash tree: %X\n", tree.Hash())

		exporter, err := tree.Export()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating exporter: %s\n", err)
			return
		}
		defer exporter.Close()

		outFile, err := os.Create(strconv.Itoa(version) + ".gob")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating file: %s\n", err)
			return
		}
		defer outFile.Close()

		encoder := gob.NewEncoder(outFile)

		for {
			node, err := exporter.Next()
			if err == iavl.ErrorExportDone {
				fmt.Println("export done")
				break
			} else if err != nil {
				fmt.Fprintf(os.Stderr, "Error export node: %s\n", err)
				return
			}

			if err := encoder.Encode(node); err != nil {
				fmt.Fprintf(os.Stderr, "Error encode node: %s\n", err)
				return
			}
		}
	case "import":
		dir, prefix := args[1], args[2]
		tree, err := CreateTree(dir, []byte(prefix))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating tree: %s\n", err)
			return
		}
		fmt.Printf("Hash tree: %X\n", tree.Hash())

		importer, err := tree.Import(int64(version))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating importer: %s\n", err)
			return
		}
		defer importer.Close()

		inFile, err := os.Open(strconv.Itoa(version) + ".gob")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error open file: %s\n", err)
			return
		}
		defer inFile.Close()

		decoder := gob.NewDecoder(inFile)

		for {
			node := &iavl.ExportNode{}
			err := decoder.Decode(node)
			if err == io.EOF {
				break
			} else if err != nil {
				fmt.Fprintf(os.Stderr, "Error decoding node: %s\n", err)
				return
			}

			if node.Key == nil {
				node.Key = []byte{}
			}
			if node.Height == 0 && node.Value == nil {
				node.Value = []byte{}
			}

			if err := importer.Add(node); err != nil {
				fmt.Fprintf(os.Stderr, "Error importing node: %s\n", err)
				return
			}
		}

		err = importer.Commit()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error commit import: %s\n", err)
			return
		}
		importer.Close()
		fmt.Println("import done")
	default:
		fmt.Fprintln(os.Stderr, `Error: Invalid command.

Usage:
  iaviewer <command> <database_path> [additional parameters]

Available commands:
  versions      - Show available tree versions
  data          - Display tree data for a specific version
  shape         - Show the structure of the IAVL tree
  tree-hash     - Get the hash of the tree for a specific version
  delete-to     - Delete versions up to the specified one
  delete-tree   - Delete the entire tree
  export        - Export tree data to a file
  import        - Import tree data from a file

Example usage:
  iaviewer versions ./bns-a.db ""
  iaviewer data ./bns-a.db "" 190257
  iaviewer shape ./bns-a.db "" 190258
  iaviewer delete-to .app/data/application.db "s/k:mint/" 4 3

For more information, refer to the documentation.`)

	}
}

func deleteKeysWithPrefix(prefix string, db *dbm.GoLevelDB) error {
	itr, err := db.Iterator(nil, nil)
	if err != nil {
		return fmt.Errorf("error creating iterator: %v", err)
	}
	defer itr.Close()

	batch := db.NewBatch()
	defer batch.Close()

	for itr.Valid() {
		key := itr.Key()

		if bytes.HasPrefix(key, []byte(prefix)) {
			if err := batch.Delete(key); err != nil {
				return fmt.Errorf("error deleting key %s: %v", string(key), err)
			}
		}

		itr.Next()
	}

	if err := itr.Error(); err != nil {
		return fmt.Errorf("error during iteration: %v", err)
	}

	// Apply batch deletion
	if err := batch.WriteSync(); err != nil {
		return fmt.Errorf("error applying deletion: %v", err)
	}

	return nil
}

func OpenDB(dir string) (dbm.DB, error) {
	switch {
	case strings.HasSuffix(dir, ".db"):
		dir = dir[:len(dir)-3]
	case strings.HasSuffix(dir, ".db/"):
		dir = dir[:len(dir)-4]
	default:
		return nil, fmt.Errorf("database directory must end with .db")
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
	db, err := dbm.NewGoLevelDB(name, dir[:cut], nil)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func OpenGoLevelDB(dir string) (*dbm.GoLevelDB, error) {
	switch {
	case strings.HasSuffix(dir, ".db"):
		dir = dir[:len(dir)-3]
	case strings.HasSuffix(dir, ".db/"):
		dir = dir[:len(dir)-4]
	default:
		return nil, fmt.Errorf("database directory must end with .db")
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
	db, err := dbm.NewGoLevelDB(name, dir[:cut], nil)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func PrintDBStats(db dbm.DB) {
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

func CreateTree(dir string, prefix []byte) (*iavl.MutableTree, error) {
	db, err := OpenDB(dir)
	if err != nil {
		return nil, err
	}
	if len(prefix) != 0 {
		db = dbm.NewPrefixDB(db, prefix)
	}

	tree := iavl.NewMutableTree(db, DefaultCacheSize, false, log.NewLogger(os.Stdout))
	return tree, err
}

// CreateAndLoadTree loads an iavl tree from the directory
// If version is 0, load latest, otherwise, load named version
// The prefix represents which iavl tree you want to read. The iaviwer will always set a prefix.
func CreateAndLoadTree(dir string, version int, prefix []byte) (*iavl.MutableTree, error) {
	db, err := OpenDB(dir)
	if err != nil {
		return nil, err
	}
	if len(prefix) != 0 {
		db = dbm.NewPrefixDB(db, prefix)
	}

	tree := iavl.NewMutableTree(db, DefaultCacheSize, false, log.NewLogger(os.Stdout))
	latest, err := tree.LoadVersion(int64(version))
	if version == 0 {
		version = int(latest)
	}
	fmt.Printf("Got version: %d\n", version)
	return tree, err
}

func PrintKeys(tree *iavl.MutableTree) {
	fmt.Println("Printing all keys with hashed values (to detect diff)")
	tree.Iterate(func(key []byte, value []byte) bool { //nolint:errcheck
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
		return encodeID(key)
	}
	prefix := key[:cut]
	id := key[cut+1:]
	return fmt.Sprintf("%s:%s", encodeID(prefix), encodeID(id))
}

// casts to a string if it is printable ascii, hex-encodes otherwise
func encodeID(id []byte) string {
	for _, b := range id {
		if b < 0x20 || b >= 0x80 {
			return strings.ToUpper(hex.EncodeToString(id))
		}
	}
	return string(id)
}

func PrintShape(tree *iavl.MutableTree) {
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
	if len(id) == 0 {
		return fmt.Sprintf("%s<nil>", prefix)
	}
	return fmt.Sprintf("%s%s", prefix, parseWeaveKey(id))
}
