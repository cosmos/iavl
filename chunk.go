package iavl

import "sort"

// Chunk is a list of ordered nodes
// It can be sorted, merged, exported from a tree and
// used to generate a new tree
type Chunk []OrderedNodeData

// OrderedNodeData is the data to recreate a leaf node,
// along with a SortOrder to define a BFS insertion order
type OrderedNodeData struct {
	SortOrder uint64
	NodeData
}

// NewOrderedNode creates the data from a leaf node
func NewOrderedNode(leaf *Node, prefix uint64) OrderedNodeData {
	return OrderedNodeData{
		SortOrder: prefix,
		NodeData: NodeData{
			Key:   leaf.key,
			Value: leaf.value,
		},
	}
}

// GetChunkHashes returns all the
func GetChunkHashes(tree *Tree, depth int) [][]byte {
	// TODO: handle case besides depth=0
	return [][]byte{tree.Hash()}
}

// GetChunk finds the count-th subtree at depth and
// generates a Chunk for that data
func GetChunk(tree *Tree, depth, count int) Chunk {
	// TODO: handle case besides depth=0, chunk=0
	node := tree.root
	return getChunk(node, 0, 0)
}

// getChunk takes a node and serializes all nodes below it
//
// As it is part of a larger tree, prefix defines the path
// up to this point, and depth the current depth
// (which defines where we add to the prefix)
//
// TODO: make this more efficient, *Chunk as arg???
func getChunk(node *Node, prefix uint64, depth uint) Chunk {
	if node.isLeaf() {
		return Chunk{NewOrderedNode(node, prefix)}
	}
	res := make(Chunk, 0, node.size)
	if node.leftNode != nil {
		left := getChunk(node.leftNode, prefix, depth+1)
		res = append(res, left...)
	}
	if node.rightNode != nil {
		offset := prefix + 1<<depth
		right := getChunk(node.rightNode, offset, depth+1)
		res = append(res, right...)
	}
	return res
}

// Sort does an inline quicksort
func (c Chunk) Sort() {
	sort.Slice(c, func(i, j int) bool {
		return c[i].SortOrder < c[j].SortOrder
	})
}

// Merge does a merge sort of the two slices,
// assuming they were already in sorted order
func Merge(left, right Chunk) Chunk {
	size, i, j := len(left)+len(right), 0, 0
	slice := make([]OrderedNodeData, size)

	for k := 0; k < size; k++ {
		if i > len(left)-1 && j <= len(right)-1 {
			slice[k] = right[j]
			j++
		} else if j > len(right)-1 && i <= len(left)-1 {
			slice[k] = left[i]
			i++
		} else if left[i].SortOrder < right[j].SortOrder {
			slice[k] = left[i]
			i++
		} else {
			slice[k] = right[j]
			j++
		}
	}
	return Chunk(slice)
}

// CalculateRoot creates a temporary in-memory
// iavl tree to calculate the root hash of inserting
// all the nodes
func (c Chunk) CalculateRoot() []byte {
	test := NewTree(2*len(c), nil)
	c.PopulateTree(test)
	return test.Hash()
}

// PopulateTree adds all the chunks in order to the given tree
func (c Chunk) PopulateTree(empty *Tree) {
	for _, data := range c {
		empty.Set(data.Key, data.Value)
	}
}
