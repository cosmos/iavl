package iavl

// KeyValue groups together a key and a value for return codes
type KeyValue struct {
	Key   []byte
	Value []byte
}

// SerializeFunc is any implementation that can serialize
// an iavl Tree
type SerializeFunc func(*Tree) []KeyValue

// Restore will take an (empty) tree restore it
// from the keys returned from a SerializeFunc
func Restore(empty *Tree, kvs []KeyValue) {
	for _, kv := range kvs {
		empty.Set(kv.Key, kv.Value)
	}
	empty.Hash()
}

// InOrderSerialize returns all key-values in the
// key order (as stored). May be nice to read, but
// when recovering, it will create a different.
func InOrderSerialize(tree *Tree) []KeyValue {
	res := make([]KeyValue, 0, tree.Size())
	tree.Iterate(func(key, value []byte) bool {
		kv := KeyValue{Key: key, Value: value}
		res = append(res, kv)
		return false
	})
	return res
}

// StableSerialize exports the key value pairs of the tree
// in an order, such that when Restored from those keys, the
// new tree would have the same structure (and thus same
// shape) as the original tree.
//
// the algorithm is basically this: take the leftmost node
// of the left half and the leftmost node of the righthalf.
// Then go down a level...
// each time adding leftmost node of the right side.
// (depth first search)
//
// Imagine 8 nodes in a balanced tree, split in half each time
// 1
// 1, 5
// 1, 5, 3, 7
// 1, 5, 3, 7, 2, 4, 6, 8
func StableSerialize(tree *Tree) []KeyValue {
	top := tree.root
	if top == nil {
		return nil
	}

	// store all pending nodes for depth-first search
	queue := make([]*Node, 0, tree.Size())
	queue = append(queue, top)

	// to store all results - started with
	res := make([]KeyValue, 0, tree.Size())
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
			kv := KeyValue{Key: r.key, Value: r.value}
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

func leftmost(node *Node) *KeyValue {
	for isInner(node) {
		node = node.leftNode
	}
	if node == nil {
		return nil
	}
	return &KeyValue{Key: node.key, Value: node.value}
}
