package iavl

import (
	"bytes"
	"sort"
)

type GetNode = func(hash []byte) (*Node, error)

// KVOperation includes three kinds of operations to the kv pairs:
// Delete: delete=true and value=nil
// Insert: original=nil
// Update: both value and original are not nil
type KVOperation struct {
	delete   bool
	key      []byte
	value    []byte
	original []byte
}

type DiffOptions struct {
	// Predecessor specifies the version and the versions before that should be skipped during the diff process,
	// it's to prevent unneeded traversal in pruning mode.
	Predecessor int64
	// If PruneMode is true, the diff process stop as soon as orphaned nodes becomes empty.
	PruneMode bool
}

func DiffFull() DiffOptions {
	return DiffOptions{}
}

func DiffForPruning(predecessor int64) DiffOptions {
	return DiffOptions{
		Predecessor: predecessor,
		PruneMode:   true,
	}
}

// DiffTree diff two versions of the iavl tree to find out the orphaned and new nodes.
// The tuple of (orphaned, new) is passed to the handle callback, it can return a boolean to specify
// should we stop the diff process.
// Accepts nil input for empty tree.
// Contract:
// - the (orphaned, new) nodes are at the same height.
// - all the nodes are persisted nodes.
func DiffTree(nodeGetter GetNode, root1, root2 *Node, handle func([]*Node, []*Node) error, opts DiffOptions) error {
	// skipping nodes created at or before predecessor
	if root1 != nil && root1.version <= opts.Predecessor {
		root1 = nil
	}
	if root2 != nil && root2.version <= opts.Predecessor {
		root2 = nil
	}

	if root1 == nil && root2 == nil {
		// both empty, nothing to do
		return nil
	}

	// handle empty trees
	var l1, l2 *layer
	switch {
	case root1 == nil:
		l2 = newLayer(root2)
		l1 = newEmptyLayer(l2.height)
	case root2 == nil:
		l1 = newLayer(root1)
		l2 = newEmptyLayer(l1.height)
	default:
		l1 = newLayer(root1)
		l2 = newLayer(root2)
	}

	for l1.height > l2.height {
		if err := handle(l1.nodes, nil); err != nil {
			return err
		}
		if err := l1.moveToNextLayer(nodeGetter, opts.Predecessor); err != nil {
			return err
		}
	}

	for l2.height > l1.height {
		if err := handle(nil, l2.nodes); err != nil {
			return err
		}
		if err := l2.moveToNextLayer(nodeGetter, opts.Predecessor); err != nil {
			return err
		}
	}

	for {
		// l1 l2 at the same height now
		orphaned, new := diffNodes(l1.nodes, l2.nodes)
		if err := handle(orphaned, new); err != nil {
			return err
		}

		if l1.isLeaf() {
			break
		}

		// reset nodes to remove the common nodes(sub-trees)
		l1.nodes = orphaned
		l2.nodes = new

		if opts.PruneMode && l1.isEmpty() {
			// nothing else to see in tree1, no more orphaned nodes, only new ones,
			// that's enough for pruning mode.
			break
		}

		if err := l1.moveToNextLayer(nodeGetter, opts.Predecessor); err != nil {
			return err
		}
		if err := l2.moveToNextLayer(nodeGetter, opts.Predecessor); err != nil {
			return err
		}
	}
	return nil
}

// StateChanges extract state changes between two versions of iavl tree
func StateChanges(nodeGetter GetNode, root1, root2 *Node) ([]KVOperation, error) {
	var ops []KVOperation
	if err := DiffTree(nodeGetter, root1, root2, func(orphaned, new []*Node) error {
		// both orphaned and new nodes are at the same height, and we only care about leaf nodes here
		// so if there's one leaf node, we need to do the work.
		if (len(orphaned) > 0 && orphaned[0].isLeaf()) || (len(new) > 0 && new[0].isLeaf()) {
			ops = diffLeafNodes(orphaned, new)
		}
		return nil
	}, DiffFull()); err != nil {
		return nil, err
	}
	return ops, nil
}

type layer struct {
	height       int8
	nodes        []*Node
	pendingNodes []*Node
}

func newLayer(root *Node) *layer {
	return &layer{
		height: root.subtreeHeight,
		nodes:  []*Node{root},
	}
}

func newEmptyLayer(height int8) *layer {
	return &layer{height: height}
}

func (l *layer) isLeaf() bool {
	return l.height == 0
}

func (l *layer) isEmpty() bool {
	return len(l.nodes)+len(l.pendingNodes) == 0
}

// Contract: l.height must be larger than 0.
// predecessor filter subtrees to visit.
func (l *layer) moveToNextLayer(nodeGetter GetNode, predecessor int64) error {
	if l.height <= 0 {
		panic("already at leaf layer")
	}
	nodes := make([]*Node, 0, len(l.nodes)*2+len(l.pendingNodes))
	pendingNodes := make([]*Node, 0)
	for _, node := range l.nodes {
		left, err := nodeGetter(node.leftHash)
		if err != nil {
			return err
		}
		if left.version > predecessor {
			if left.subtreeHeight == l.height-1 {
				nodes = append(nodes, left)
			} else {
				pendingNodes = append(pendingNodes, left)
			}
		}

		right, err := nodeGetter(node.rightHash)
		if err != nil {
			return err
		}
		if right.version > predecessor {
			if right.subtreeHeight == l.height-1 {
				nodes = append(nodes, right)
			} else {
				pendingNodes = append(pendingNodes, right)
			}
		}
	}

	l.height--

	nodes = append(nodes, l.pendingNodes...)
	// merge sorted lists
	sort.SliceStable(nodes, func(i, j int) bool {
		return bytes.Compare(nodes[i].key, nodes[j].key) == -1
	})
	l.nodes = nodes
	l.pendingNodes = pendingNodes
	return nil
}

// Contract: both input lists are at the same height, and sorted by node.key
// return: (orphaned, new)
// orphaned: exists in nodes1 but not in nodes2
// new: don't exists in nodes1 but in nodes2
func diffNodes(nodes1 []*Node, nodes2 []*Node) ([]*Node, []*Node) {
	var i1, i2 int
	orphaned := make([]*Node, 0)
	new := make([]*Node, 0)
	for {
		if i1 > len(nodes1)-1 {
			// nodes1 is exhausted
			new = append(new, nodes2[i2:]...)
			break
		}
		if i2 > len(nodes2)-1 {
			// nodes2 is exhausted
			orphaned = append(orphaned, nodes1[i1:]...)
			break
		}
		node1 := nodes1[i1]
		node2 := nodes2[i2]
		switch bytes.Compare(node1.key, node2.key) {
		case 0:
			// we assume they are the same nodes without comparing hashes
			// if they have the same (height, key, version).
			if node1.version != node2.version {
				// overridden by same key
				orphaned = append(orphaned, node1)
				new = append(new, node2)
			}
			i1++
			i2++
		case -1:
			// proceed to next node in nodes1 until catch up with nodes2
			orphaned = append(orphaned, node1)
			i1++
		default:
			// proceed to next node in nodes2 until catch up with nodes1
			new = append(new, node2)
			i2++
		}
	}
	return orphaned, new
}

// diffLeafNodes find differences in leaf nodes and reconstruct the operations.
// Contract: input nodes are all leaf nodes, sorted by node.key
func diffLeafNodes(nodes1, nodes2 []*Node) []KVOperation {
	var i1, i2 int
	result := make([]KVOperation, 0)
	for {
		if i1 > len(nodes1)-1 {
			// nodes1 is exhausted, the remaining nodes2 are insertions.
			for _, n := range nodes2[i2:] {
				result = append(result, KVOperation{
					key:   n.key,
					value: n.value,
				})
			}
			break
		}
		if i2 > len(nodes2)-1 {
			// nodes2 is exhausted, the remaining nodes1 are deletions.
			for _, n := range nodes1[i1:] {
				result = append(result, KVOperation{
					key:      n.key,
					original: n.value,
					delete:   true,
				})
			}
			break
		}
		n1 := nodes1[i1]
		n2 := nodes2[i2]
		switch bytes.Compare(n1.key, n2.key) {
		case 0:
			// update
			result = append(result, KVOperation{
				key:      n1.key,
				value:    n2.value,
				original: n1.value,
			})
			i1++
			i2++
		case -1:
			// proceed to next node in nodes1 until catch up with nodes2
			result = append(result, KVOperation{
				key:      n1.key,
				original: n1.value,
				delete:   false,
			})
			i1++
		default:
			// proceed to next node in nodes2 until catch up with nodes1
			result = append(result, KVOperation{
				key:   n2.key,
				value: n2.value,
			})
			i2++
		}
	}
	return result
}
