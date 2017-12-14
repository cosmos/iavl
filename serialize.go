package iavl

// NodeData groups together a key, value and depth.
type NodeData struct {
	Key   []byte
	Value []byte
	Depth uint8
}

// SerializeFunc is any implementation that can serialize
// an iavl Node and its descendants.
type SerializeFunc func(*Tree, *Node) []NodeData

// RestoreFunc is an implementation that can restore an iavl tree from
// NodeData.
type RestoreFunc func(*Tree, []NodeData)

// Restore will take an (empty) tree restore it
// from the keys returned from a SerializeFunc.
func Restore(empty *Tree, kvs []NodeData) {
	for _, kv := range kvs {
		empty.Set(kv.Key, kv.Value)
	}
	empty.Hash()
}

func RestoreUsingDepth(empty *Tree, kvs []NodeData) {
	// First we need to know what the max depth is. Ideally this should be
	// available without having to iterate over the keys.
	maxDepth := uint8(0)
	for _, kv := range kvs {
		if kv.Depth > maxDepth {
			maxDepth = kv.Depth
		}
	}

	// Create an array of arrays of nodes of size maxDepth + 1. We're going to
	// store each depth in here, forming a kind of pyramid.
	depths := make([][]*Node, maxDepth+1)

	// Go through all the leaf nodes, grouping them in pairs and creating their
	// parents at depth - 1.
	for _, kv := range kvs {
		var (
			// Left and right nodes.
			l *Node = nil
			r *Node = NewNode(kv.Key, kv.Value, 0)
		)

		d := kv.Depth                    // Current depth.
		depths[d] = append(depths[d], r) // Add the leaf node to this depth.
		nodes := depths[d]               // List of nodes at this depth.

		// If the nodes at this level are uneven after adding a node to it, it
		// means we have to wait for another node to be appended before we have
		// a pair.
		if len(nodes)%2 != 0 {
			continue
		}

		// Now we have both a left and a right.
		l = nodes[len(nodes)-1-1]

		// If the current depth is the greatest, build the parent of the two
		// children.
		if int(d) == len(depths)-1 {
			depths[d-1] = append(depths[d-1], makeParentNode(l, r))
		}
	}

	// Now take care of inner nodes up to the root.
	for d := maxDepth - 1; d > 0; d-- {
		nodes := depths[d]
		for i := 0; i < len(nodes); i += 2 {
			depths[d-1] = append(depths[d-1], makeParentNode(nodes[i], nodes[i+1]))
		}
	}
	empty.root = depths[0][0]
	empty.Hash()
}

func makeParentNode(l, r *Node) *Node {
	return &Node{
		key:       leftmost(r).Key,
		height:    maxInt8(l.height, r.height) + 1,
		size:      l.size + r.size,
		leftNode:  l,
		rightNode: r,
		version:   0,
	}
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

// StableSerializeBFS serializes the tree in a breadth-first manner.
func StableSerializeBFS(t *Tree, root *Node) []NodeData {
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

// StableSerializeFrey exports the key value pairs of the tree
// in an order, such that when Restored from those keys, the
// new tree would have the same structure (and thus same
// shape) as the original tree.
//
// the algorithm is basically this: take the leftmost node
// of the left half and the leftmost node of the righthalf.
// Then go down a level...
// each time adding leftmost node of the right side.
// (bredth first search)
//
// Imagine 8 nodes in a balanced tree, split in half each time
// 1
// 1, 5
// 1, 5, 3, 7
// 1, 5, 3, 7, 2, 4, 6, 8
func StableSerializeFrey(t *Tree, top *Node) []NodeData {
	if top == nil {
		return nil
	}
	size := top.size

	// store all pending nodes for depth-first search
	queue := make([]*Node, 0, size)
	queue = append(queue, top)

	// to store all results - started with
	res := make([]NodeData, 0, size)
	left := leftmost(top)
	if left != nil {
		res = append(res, *left)
	}

	var n *Node
	for len(queue) > 0 {
		// pop
		n, queue = queue[0], queue[1:]

		// l := n.getLeftNode(tree)
		l := n.leftNode
		if isInner(l) {
			queue = append(queue, l)
		}

		// r := n.getRightNode(tree)
		r := n.rightNode
		if isInner(r) {
			queue = append(queue, r)
			left = leftmost(r)
			if left != nil {
				res = append(res, *left)
			}
		} else if isLeaf(r) {
			kv := NodeData{Key: r.key, Value: r.value}
			res = append(res, kv)
		}
	}

	return res
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
