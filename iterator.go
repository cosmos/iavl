package iavl

import "bytes"

type iterator interface {
	// Domain returns the start (inclusive) and end (exclusive) limits of the iterator.
	// CONTRACT: start, end readonly []byte
	Domain() (start []byte, end []byte)

	// Valid returns whether the current iterator is valid. Once invalid, the Iterator remains
	// invalid forever.
	Valid() bool

	// Next moves the iterator to the next key in the database, as defined by order of iteration.
	// If Valid returns false, this method will panic.
	Next()

	// Key returns the key at the current position. Panics if the iterator is invalid.
	// CONTRACT: key readonly []byte
	Key() (key []byte)

	// Value returns the value at the current position. Panics if the iterator is invalid.
	// CONTRACT: value readonly []byte
	Value() (value []byte)

	// Error returns the last error encountered by the iterator, if any.
	Error() error

	// Close closes the iterator, relasing any allocated resources.
	Close() error
}

var _ iterator = (*Iterator)(nil)

type Iterator struct {
	tree       *Tree
	start, end []byte // iteration domain
	ascending  bool   // ascending traversal
	inclusive  bool   // end key inclusiveness

	stack   []*Node
	started bool

	key, value []byte // current key, value
	err        error  // current error
	valid      bool   // iteration status
}

func (i *Iterator) Domain() (start []byte, end []byte) {
	return i.start, i.end
}

func (i *Iterator) Valid() bool {
	return i.valid
}

func (i *Iterator) Next() {
	if !i.valid {
		return
	}
	if len(i.stack) == 0 {
		i.valid = false
		return
	}
	if i.ascending {
		i.stepAscend()
	} else {
		i.stepDescend()
	}
	i.started = true
}

func (i *Iterator) push(node *Node) {
	i.stack = append(i.stack, node)
}

func (i *Iterator) pop() (node *Node) {
	node = i.stack[len(i.stack)-1]
	i.stack = i.stack[:len(i.stack)-1]
	return
}

func (i *Iterator) stepAscend() {
	var n *Node
	for {
		n = i.pop()
		if i.isPastEnd(n.key) {
			i.valid = false
			return
		}
		if n.isLeaf() {
			if !i.started && bytes.Compare(n.key, i.start) < 0 {
				continue
			} else {
				break
			}
		}
		right, err := n.getRightNode(i.tree)
		if err != nil {
			i.err = err
			i.valid = false
			return
		}

		if bytes.Compare(i.start, n.key) < 0 {
			left, err := n.getLeftNode(i.tree)
			if err != nil {
				i.err = err
				i.valid = false
				return
			}
			i.push(right)
			i.push(left)
		} else {
			i.push(right)
		}

	}
	i.key = n.key
	i.value = n.value
}

func (i *Iterator) stepDescend() {

}

func (i *Iterator) firstDescend() {
	var n *Node
	for n = i.tree.root; !n.isLeaf(); {
		i.stack = append(i.stack, n)
		n = n.rightNode
	}
	i.key = n.key
	i.value = n.value
}

func (i *Iterator) isPastEnd(key []byte) bool {
	if i.end == nil {
		return false
	}
	if i.inclusive {
		return bytes.Compare(key, i.end) > 0
	}
	return bytes.Compare(key, i.end) >= 0
}

func (i *Iterator) Key() (key []byte) {
	return i.key
}

func (i *Iterator) Value() (value []byte) {
	return i.value
}

func (i *Iterator) Error() error {
	return i.err
}

func (i *Iterator) Close() error {
	i.stack = nil
	i.valid = false
	return i.err
}

func (tree *Tree) Iterator(start, end []byte, ascending bool) (*Iterator, error) {
	return &Iterator{
		tree:      tree,
		start:     start,
		end:       end,
		ascending: ascending,
		inclusive: true,
		valid:     true,
		stack:     []*Node{tree.root},
	}, nil
}
