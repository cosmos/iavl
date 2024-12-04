package iavl

type pruningContext struct {
	// pruneVersion is the version to prune to.
	pruneVersion uint64
	sql          *SqliteDb
	checkpoints  VersionRange
}

func NewPruningContext(path string, checkpoints VersionRange, pruneVersion int64) {

}

func (p *pruningContext) Execute() error {
	return nil
}
