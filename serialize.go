package iavl

// NodeData groups together a key, value and depth.
type NodeData struct {
	Key   []byte
	Value []byte
	Depth uint8
}

// Serializer is anything that can serialize and restore a *Tree.
type Serializer interface {
	Serialize(*Tree, *Node) []NodeData
	Restore(*Tree, []NodeData)
}

// NewSerializer returns a new serializer using the default algorithm.
func NewSerializer() Serializer {
	return &inOrderSerializer{}
}

// inOrderSerializer serializes a tree in sort order and restores trees
// bottom-up using depth information.
type inOrderSerializer struct{}

var _ Serializer = &inOrderSerializer{}

func (s *inOrderSerializer) Serialize(t *Tree, root *Node) []NodeData {
	return InOrderSerialize(t, root)
}

func (s *inOrderSerializer) Restore(empty *Tree, kvs []NodeData) {
	// Create an array of arrays of nodes. We're going to store each depth in
	// here, forming a kind of pyramid.
	depths := [][]*Node{}

	// Go through all the leaf nodes, grouping them in pairs and creating their
	// parents recursively.
	for _, kv := range kvs {
		var (
			// Left and right nodes.
			l     *Node = nil
			r     *Node = NewNode(kv.Key, kv.Value, 1)
			depth uint8 = kv.Depth
		)
		// Create depths as needed.
		for len(depths) < int(depth)+1 {
			depths = append(depths, []*Node{})
		}
		depths[depth] = append(depths[depth], r) // Add the leaf node to this depth.

		// If the nodes at this level are uneven after adding a node to it, it
		// means we have to wait for another node to be appended before we have
		// a pair. If we do have a pair, go up the tree until we don't.
		for d := depth; len(depths[d])%2 == 0; d-- {
			nodes := depths[d] // List of nodes at this depth.

			l = nodes[len(nodes)-1-1]
			r = nodes[len(nodes)-1]

			depths[d-1] = append(depths[d-1], &Node{
				key:       leftmost(r).Key,
				height:    maxInt8(l.height, r.height) + 1,
				size:      l.size + r.size,
				leftNode:  l,
				rightNode: r,
				version:   1,
			})
		}
	}
	empty.root = depths[0][0]
	empty.Hash()
}

// InOrderSerialize returns all key-values in the
// key order (as stored). May be nice to read, but
// when recovering, it will create a different.
func InOrderSerialize(t *Tree, root *Node) []NodeData {
	res := make([]NodeData, 0, root.size)
	root.traverseWithDepth(t, true, func(node *Node, depth uint8) bool {
		if node.height == 0 {
			kv := NodeData{Key: node.key, Value: node.value, Depth: depth}
			res = append(res, kv)
		}
		return false
	})
	return res
}

// breadthFirstSerializer can serialize a tree in a breadth-first manner.
type breadthFirstSerializer struct{}

var _ Serializer = &breadthFirstSerializer{}

func (s *breadthFirstSerializer) Restore(empty *Tree, kvs []NodeData) {
	for _, kv := range kvs {
		empty.Set(kv.Key, kv.Value)
	}
	empty.Hash()
}

func (s *breadthFirstSerializer) Serialize(t *Tree, root *Node) []NodeData {
	if root == nil {
		return nil
	}

	size := root.size
	visited := map[string][]byte{}
	keys := make([][]byte, 0, size)
	numKeys := -1

	// Breadth-first search. At every depth, add keys in search order. Keep
	// going as long as we find keys at that depth. When we reach a leaf, set
	// its value in the visited map.
	// Since we have an AVL+ tree, the inner nodes contain only keys and not
	// values, while the leaves contain both. Note also that there are N-1 inner
	// nodes for N keys, so one of the leaf keys is only set once we reach the leaves
	// of the tree.
	for depth := uint(0); len(keys) > numKeys; depth++ {
		numKeys = len(keys)
		root.traverseDepth(t, depth, func(node *Node) {
			if _, ok := visited[string(node.key)]; !ok {
				keys = append(keys, node.key)
				visited[string(node.key)] = nil
			}
			if node.isLeaf() {
				visited[string(node.key)] = node.value
			}
		})
	}

	nds := make([]NodeData, size)
	for i, k := range keys {
		nds[i] = NodeData{k, visited[string(k)], 0}
	}
	return nds
}

func isInner(n *Node) bool {
	return n != nil && !n.isLeaf()
}

func isLeaf(n *Node) bool {
	return n != nil && n.isLeaf()
}

func leftmost(node *Node) *NodeData {
	for isInner(node) {
		node = node.leftNode
	}
	if node == nil {
		return nil
	}
	return &NodeData{Key: node.key, Value: node.value}
}
