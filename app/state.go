package app

import "github.com/tendermint/merkleeyes/iavl"

// State represents the app states, separating the commited state (for queries)
// from the working state (for CheckTx and AppendTx)
type State struct {
	committed  iavl.Tree
	deliverTx  iavl.Tree
	checkTx    iavl.Tree
	persistent bool
}

func NewState(tree iavl.Tree, persistent bool) State {
	return State{
		committed:  tree,
		deliverTx:  tree.Copy(),
		checkTx:    tree.Copy(),
		persistent: persistent,
	}
}

func (s State) Committed() iavl.Tree {
	return s.committed
}

func (s State) Append() iavl.Tree {
	return s.deliverTx
}

func (s State) Check() iavl.Tree {
	return s.checkTx
}

// Commit stores the current Append() state as committed
// starts new Append/Check state, and
// returns the hash for the commit
func (s *State) Commit() []byte {
	var hash []byte
	if s.persistent {
		hash = s.deliverTx.Save()
	} else {
		hash = s.deliverTx.Hash()
	}

	s.committed = s.deliverTx
	s.deliverTx = s.committed.Copy()
	s.checkTx = s.committed.Copy()
	return hash
}
