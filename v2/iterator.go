package iavl

import (
	"bytes"
	"fmt"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/cosmos/iavl/v2/metrics"
)

type Iterator interface {
	// Domain returns the start (inclusive) and end (exclusive) limits of the iterator.
	// CONTRACT: start, end readonly []byte
	Domain() (start []byte, end []byte)

	// Valid returns whether the current iterator is valid. Once invalid, the TreeIterator remains
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

	// Close closes the iterator, releasing any allocated resources.
	Close() error
}

var (
	_ Iterator = (*TreeIterator)(nil)
	_ Iterator = (*LeafIterator)(nil)
)

type TreeIterator struct {
	tree       *Tree
	start, end []byte // iteration domain
	ascending  bool   // ascending traversal
	inclusive  bool   // end key inclusiveness

	stack   []*Node
	started bool

	key, value []byte // current key, value
	err        error  // current error
	valid      bool   // iteration status

	metrics metrics.Proxy
}

func (i *TreeIterator) Domain() (start []byte, end []byte) {
	return i.start, i.end
}

func (i *TreeIterator) Valid() bool {
	return i.valid
}

func (i *TreeIterator) Next() {
	if i.metrics != nil {
		defer i.metrics.MeasureSince(time.Now(), "iavl_v2", "iterator", "next")
	}
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

func (i *TreeIterator) push(node *Node) {
	i.stack = append(i.stack, node)
}

func (i *TreeIterator) pop() (node *Node) {
	if len(i.stack) == 0 {
		return nil
	}
	node = i.stack[len(i.stack)-1]
	i.stack = i.stack[:len(i.stack)-1]
	return
}

func (i *TreeIterator) stepAscend() {
	var n *Node
	for {
		n = i.pop()
		if n == nil {
			i.valid = false
			return
		}
		if n.isLeaf() {
			if !i.started && bytes.Compare(n.key, i.start) < 0 {
				continue
			}
			if i.isPastEndAscend(n.key) {
				i.valid = false
				return
			}
			break
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

func (i *TreeIterator) stepDescend() {
	var n *Node
	for {
		n = i.pop()
		if n == nil {
			i.valid = false
			return
		}
		if n.isLeaf() {
			if !i.started && i.end != nil {
				res := bytes.Compare(i.end, n.key)
				// if end is inclusive and the key is greater than end, skip
				if i.inclusive && res < 0 {
					continue
				}
				// if end is not inclusive (default) and the key is greater than or equal to end, skip
				if res <= 0 {
					continue
				}
			}
			if i.isPastEndDescend(n.key) {
				i.valid = false
				return
			}
			break
		}
		left, err := n.getLeftNode(i.tree)
		if err != nil {
			i.err = err
			i.valid = false
			return
		}

		if i.end == nil || bytes.Compare(n.key, i.end) <= 0 {
			right, err := n.getRightNode(i.tree)
			if err != nil {
				i.err = err
				i.valid = false
				return
			}
			i.push(left)
			i.push(right)
		} else {
			i.push(left)
		}
	}
	i.key = n.key
	i.value = n.value
}

func (i *TreeIterator) isPastEndAscend(key []byte) bool {
	if i.end == nil {
		return false
	}
	if i.inclusive {
		return bytes.Compare(key, i.end) > 0
	}
	return bytes.Compare(key, i.end) >= 0
}

func (i *TreeIterator) isPastEndDescend(key []byte) bool {
	if i.start == nil {
		return false
	}
	return bytes.Compare(key, i.start) < 0
}

func (i *TreeIterator) Key() (key []byte) {
	return i.key
}

func (i *TreeIterator) Value() (value []byte) {
	return i.value
}

func (i *TreeIterator) Error() error {
	return i.err
}

func (i *TreeIterator) Close() error {
	i.stack = nil
	i.valid = false
	return i.err
}

type LeafIterator struct {
	sql     *SqliteDb
	itrStmt *sqlite3.Stmt
	start   []byte
	end     []byte
	valid   bool
	err     error
	key     []byte
	value   []byte
	metrics metrics.Proxy
	itrIdx  int
}

func (l *LeafIterator) Domain() (start []byte, end []byte) {
	return l.start, l.end
}

func (l *LeafIterator) Valid() bool {
	return l.valid
}

func (l *LeafIterator) Next() {
	if l.metrics != nil {
		defer l.metrics.MeasureSince(time.Now(), "iavl_v2", "iterator", "next")
	}
	if !l.valid {
		return
	}

	hasRow, err := l.itrStmt.Step()
	if err != nil {
		closeErr := l.Close()
		if closeErr != nil {
			l.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
		}
		return
	}
	if !hasRow {
		closeErr := l.Close()
		if closeErr != nil {
			l.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
		}
		return
	}
	if err = l.itrStmt.Scan(&l.key, &l.value); err != nil {
		closeErr := l.Close()
		if closeErr != nil {
			l.err = fmt.Errorf("error closing iterator: %w; %w", closeErr, err)
		}
		return
	}
}

func (l *LeafIterator) Key() (key []byte) {
	return l.key
}

func (l *LeafIterator) Value() (value []byte) {
	return l.value
}

func (l *LeafIterator) Error() error {
	return l.err
}

func (l *LeafIterator) Close() error {
	if l.valid {
		if l.metrics != nil {
			l.metrics.IncrCounter(1, "iavl_v2", "iterator", "close")
		}
		l.valid = false
		delete(l.sql.iterators, l.itrIdx)
		return l.itrStmt.Close()
	}
	return nil
}

func (tree *Tree) Iterator(start, end []byte, inclusive bool) (itr Iterator, err error) {
	if tree.storeLatestLeaves {
		leafItr := &LeafIterator{
			sql:     tree.sql,
			start:   start,
			end:     end,
			valid:   true,
			metrics: tree.metricsProxy,
		}
		// TODO: handle inclusive
		// TODO: profile re-use of some prepared statement to see if there is improvement
		leafItr.itrStmt, leafItr.itrIdx, err = tree.sql.getLeafIteratorQuery(start, end, true, inclusive)
		if err != nil {
			return nil, err
		}
		itr = leafItr
	} else {
		itr = &TreeIterator{
			tree:      tree,
			start:     start,
			end:       end,
			ascending: true,
			inclusive: inclusive,
			valid:     true,
			stack:     []*Node{tree.root},
			metrics:   tree.metricsProxy,
		}
	}

	if tree.metricsProxy != nil {
		tree.metricsProxy.IncrCounter(1, "iavl_v2", "iterator", "open")
	}
	itr.Next()
	return itr, err
}

func (tree *Tree) ReverseIterator(start, end []byte) (itr Iterator, err error) {
	if tree.storeLatestLeaves {
		leafItr := &LeafIterator{
			sql:     tree.sql,
			start:   start,
			end:     end,
			valid:   true,
			metrics: tree.metricsProxy,
		}
		// TODO: handle inclusive
		// TODO: profile re-use of some prepared statement to see if there is improvement
		leafItr.itrStmt, leafItr.itrIdx, err = tree.sql.getLeafIteratorQuery(start, end, false, false)
		if err != nil {
			return nil, err
		}
		itr = leafItr
	} else {
		itr = &TreeIterator{
			tree:      tree,
			start:     start,
			end:       end,
			ascending: false,
			inclusive: false,
			valid:     true,
			stack:     []*Node{tree.root},
			metrics:   tree.metricsProxy,
		}
	}
	if tree.metricsProxy != nil {
		tree.metricsProxy.IncrCounter(1, "iavl_v2", "iterator", "open")
	}
	itr.Next()
	return itr, nil
}
