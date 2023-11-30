package iavl

import "fmt"

// traverseOrphans traverses the orphaned nodes which were removed
// by the updates of the curVersion in the prevVersion.
func (tree *Tree) traverseOrphans(prevVersion, curVersion int64, fn func(*Node) error) error {
	if prevVersion >= curVersion {
		return fmt.Errorf("prevVersion %d >= curVersion %d", prevVersion, curVersion)
	}

	prevRoot, err := tree.sql.LoadRoot(prevVersion)
	if err != nil {
		return fmt.Errorf("failed to load previous root for version %d: %w", prevVersion, err)
	}
	curRoot, err := tree.sql.LoadRoot(curVersion)
	if err != nil {
		return fmt.Errorf("failed to load current root for version %d: %w", curVersion, err)
	}

	// traverse the current root and find the remaining nodes which the version is less or equal to prevVersion.
	remainingNodes := make(chan *Node)
	errCh := make(chan error, 2)
	defer close(errCh)

	var (
		curVersionTraverse  func(node *Node) error
		prevVersionTraverse func(node *Node) error
		subRoot             *Node
		chClosed            bool
	)

	curVersionTraverse = func(node *Node) error {
		if node.nodeKey.Version() <= prevVersion {
			remainingNodes <- node
			return nil
		}
		if node.isLeaf() {
			return nil
		}
		if err := curVersionTraverse(node.left(tree)); err != nil {
			return err
		}
		return curVersionTraverse(node.right(tree))
	}

	prevVersionTraverse = func(node *Node) error {
		if subRoot == nil {
			subRoot, chClosed = <-remainingNodes
			if !chClosed {
				return nil
			}
		}
		if subRoot.nodeKey == node.nodeKey {
			subRoot = nil
			return nil
		}
		if err := fn(node); err != nil {
			return fmt.Errorf("failed to execute fn: %w", err)
		}
		if node.isLeaf() {
			return nil
		}
		if err := prevVersionTraverse(node.left(tree)); err != nil {
			return err
		}
		return prevVersionTraverse(node.right(tree))
	}

	go func() {
		errCh <- curVersionTraverse(curRoot)
		close(remainingNodes)
	}()

	if err := prevVersionTraverse(prevRoot); err != nil {
		return err
	}

	return <-errCh
}
