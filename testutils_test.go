package iavl

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"

	mrand "math/rand"

	"github.com/tendermint/go-amino"
	cmn "github.com/tendermint/tmlibs/common"
	"github.com/tendermint/tmlibs/db"
)

func dummyPathToLeaf(t *Tree, key []byte) PathToLeaf {
	path, _, err := t.root.PathToLeaf(t, key)
	if err != nil {
		panic(err)
	}
	return path
}

func dummyLeafNode(key, val []byte) proofLeafNode {
	return proofLeafNode{key, val, 1}
}

func randstr(length int) string {
	return cmn.RandStr(length)
}

func i2b(i int) []byte {
	buf := new(bytes.Buffer)
	amino.EncodeInt32(buf, int32(i))
	return buf.Bytes()
}

func b2i(bz []byte) int {
	i, _, _ := amino.DecodeInt32(bz)
	return int(i)
}

// Convenience for a new node
func N(l, r interface{}) *Node {
	var left, right *Node
	if _, ok := l.(*Node); ok {
		left = l.(*Node)
	} else {
		left = NewNode(i2b(l.(int)), nil, 0)
	}
	if _, ok := r.(*Node); ok {
		right = r.(*Node)
	} else {
		right = NewNode(i2b(r.(int)), nil, 0)
	}

	n := &Node{
		key:       right.lmd(nil).key,
		value:     nil,
		leftNode:  left,
		rightNode: right,
	}
	n.calcHeightAndSize(nil)
	return n
}

// Setup a deep node
func T(n *Node) *Tree {
	d := db.NewDB("test", db.MemDBBackend, "")
	t := NewTree(d, 0)

	n.hashWithCount()
	t.root = n
	return t
}

// Convenience for simple printing of keys & tree structure
func P(n *Node) string {
	if n.height == 0 {
		return fmt.Sprintf("%v", b2i(n.key))
	} else {
		return fmt.Sprintf("(%v %v)", P(n.leftNode), P(n.rightNode))
	}
}

func randBytes(length int) []byte {
	key := make([]byte, length)
	// math.rand.Read always returns err=nil
	mrand.Read(key)
	return key
}

type traverser struct {
	first string
	last  string
	count int
}

func (t *traverser) view(key, value []byte) bool {
	if t.first == "" {
		t.first = string(key)
	}
	t.last = string(key)
	t.count += 1
	return false
}

func expectTraverse(t *testing.T, trav traverser, start, end string, count int) {
	if trav.first != start {
		t.Error("Bad start", start, trav.first)
	}
	if trav.last != end {
		t.Error("Bad end", end, trav.last)
	}
	if trav.count != count {
		t.Error("Bad count", count, trav.count)
	}
}

func BenchmarkImmutableAvlTreeMemDB(b *testing.B) {
	db := db.NewDB("test", db.MemDBBackend, "")
	benchmarkImmutableAvlTreeWithDB(b, db)
}

func benchmarkImmutableAvlTreeWithDB(b *testing.B, db db.DB) {
	defer db.Close()

	b.StopTimer()

	t := NewVersionedTree(db, 100000)
	for i := 0; i < 1000000; i++ {
		t.Set(i2b(int(cmn.RandInt32())), nil)
		if i > 990000 && i%1000 == 999 {
			t.SaveVersion()
		}
	}
	b.ReportAllocs()
	t.SaveVersion()

	fmt.Println("ok, starting")

	runtime.GC()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		ri := i2b(int(cmn.RandInt32()))
		t.Set(ri, nil)
		t.Remove(ri)
		if i%100 == 99 {
			t.SaveVersion()
		}
	}
}
