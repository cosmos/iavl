package iavl

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
)

// PrintTree prints the whole tree in an indented form.
func PrintTree(tree *ImmutableTree) {
	ndb, root := tree.ndb, tree.root
	printNode(ndb, root, 0)
}

func printNode(ndb *nodeDB, node *Node, indent int) {
	indentPrefix := ""
	for i := 0; i < indent; i++ {
		indentPrefix += "    "
	}

	if node == nil {
		fmt.Printf("%s<nil>\n", indentPrefix)
		return
	}
	if node.rightNode != nil {
		printNode(ndb, node.rightNode, indent+1)
	} else if node.rightHash != nil {
		rightNode := ndb.GetNode(node.rightHash)
		printNode(ndb, rightNode, indent+1)
	}

	hash := node._hash()
	fmt.Printf("%sh:%X\n", indentPrefix, hash)
	if node.isLeaf() {
		fmt.Printf("%s%X:%X (%v)\n", indentPrefix, node.key, node.value, node.height)
	}

	if node.leftNode != nil {
		printNode(ndb, node.leftNode, indent+1)
	} else if node.leftHash != nil {
		leftNode := ndb.GetNode(node.leftHash)
		printNode(ndb, leftNode, indent+1)
	}

}

func maxInt8(a, b int8) int8 {
	if a > b {
		return a
	}
	return b
}

func cp(bz []byte) (ret []byte) {
	ret = make([]byte, len(bz))
	copy(ret, bz)
	return ret
}

// Returns a slice of the same length (big endian)
// except incremented by one.
// Appends 0x00 if bz is all 0xFF.
// CONTRACT: len(bz) > 0
func cpIncr(bz []byte) (ret []byte) {
	ret = cp(bz)
	for i := len(bz) - 1; i >= 0; i-- {
		if ret[i] < byte(0xFF) {
			ret[i]++
			return
		}
		ret[i] = byte(0x00)
		if i == 0 {
			return append(ret, 0x00)
		}
	}
	return []byte{0x00}
}

type byteslices [][]byte

func (bz byteslices) Len() int {
	return len(bz)
}

func (bz byteslices) Less(i, j int) bool {
	switch bytes.Compare(bz[i], bz[j]) {
	case -1:
		return true
	case 0, 1:
		return false
	default:
		panic("should not happen")
	}
}

func (bz byteslices) Swap(i, j int) {
	bz[j], bz[i] = bz[i], bz[j]
}

func sortByteSlices(src [][]byte) [][]byte {
	bzz := byteslices(src)
	sort.Sort(bzz)
	return bzz
}

// Colors: ------------------------------------------------

const (
	ANSIReset  = "\x1b[0m"
	ANSIBright = "\x1b[1m"

	ANSIFgGreen = "\x1b[32m"
	ANSIFgBlue  = "\x1b[34m"
	ANSIFgCyan  = "\x1b[36m"
)

// color the string s with color 'color'
// unless s is already colored
func treat(s string, color string) string {
	if len(s) > 2 && s[:2] == "\x1b[" {
		return s
	}
	return color + s + ANSIReset
}

func treatAll(color string, args ...interface{}) string {
	parts := make([]string, 0, len(args))
	for _, arg := range args {
		parts = append(parts, treat(fmt.Sprintf("%v", arg), color))
	}
	return strings.Join(parts, "")
}

func Green(args ...interface{}) string {
	return treatAll(ANSIFgGreen, args...)
}

func Blue(args ...interface{}) string {
	return treatAll(ANSIFgBlue, args...)
}

func Cyan(args ...interface{}) string {
	return treatAll(ANSIFgCyan, args...)
}

// ColoredBytes takes in the byte that you would like to show as a string and byte
// and will display them in a human readable format.
// If the environment variable TENDERMINT_IAVL_COLORS_ON is set to a non-empty string then different colors will be used for bytes and strings.
func ColoredBytes(data []byte, textColor, bytesColor func(...interface{}) string) string {
	colors := os.Getenv("TENDERMINT_IAVL_COLORS_ON")
	if colors == "" {
		for _, b := range data {
			return string(b)
		}
	}
	s := ""
	for _, b := range data {
		if 0x21 <= b && b < 0x7F {
			s += textColor(string(b))
		} else {
			s += bytesColor(fmt.Sprintf("%02X", b))
		}
	}
	return s
}

// -------------------------------------------------

const (
	strChars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz" // 62 characters
)

func RandStr(length int) string {
	chars := []byte{}
MAIN_LOOP:
	for {
		val := rand.Int63()
		for i := 0; i < 10; i++ {
			v := int(val & 0x3f) // rightmost 6 bits
			if v >= 62 {         // only 62 characters in strChars
				val >>= 6
				continue
			} else {
				chars = append(chars, strChars[v])
				if len(chars) == length {
					break MAIN_LOOP
				}
				val >>= 6
			}
		}
	}

	return string(chars)
}

func Bytes(n int) []byte {
	// cRandBytes isn't guaranteed to be fast so instead
	// use random bytes generated from the internal PRNG
	bs := make([]byte, n)
	for i := 0; i < len(bs); i++ {
		bs[i] = byte(rand.Int() & 0xFF) //nolint: gosec Turn off gosec here because this is for testing
	}
	return bs
}

// Perm returns a pseudo-random permutation of n integers in [0, n).
func Perm(n int) []int {
	var mutex sync.Mutex
	mutex.Lock()
	perm := rand.Perm(n)
	mutex.Unlock()
	return perm
}
