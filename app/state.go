package app

import merkle "github.com/tendermint/go-merkle"

// State represents the app states, separating the commited state (for queries)
// from the working state (for CheckTx and AppendTx)
type State struct {
	committed merkle.Tree
	deliverTx merkle.Tree
	checkTx   merkle.Tree
}

func NewState(tree merkle.Tree, persistent bool) State {
	return State{
		committed: tree,
		deliverTx: tree.Copy(),
		checkTx:   tree.Copy(),
	}
}

func (s State) Committed() merkle.Tree {
	return s.committed
}

func (s State) Append() merkle.Tree {
	return s.deliverTx
}

func (s State) Check() merkle.Tree {
	return s.checkTx
}

// Commit stores the current Append() state as committed
// starts new Append/Check state, and
// returns the hash for the commit
func (s *State) Commit() []byte {
	// TODO: use save, broken for now, cuz it requires a backing db
	hash := s.deliverTx.Hash()
	s.committed = s.deliverTx
	s.deliverTx = s.committed.Copy()
	s.checkTx = s.committed.Copy()
	return hash
}
