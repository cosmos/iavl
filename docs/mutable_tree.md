# Mutable Tree

### Structure

The MutableTree struct is a wrapper around ImmutableTree to allow for updates that get stored in successive versions.

The MutableTree stores the last saved ImmutableTree and the current working tree in its struct while all other saved, available versions are accessible from the nodeDB.

```golang
// MutableTree is a persistent tree which keeps track of versions.
type MutableTree struct {
	*ImmutableTree                  // The current, working tree.
	lastSaved      *ImmutableTree   // The most recently saved tree.
	orphans        map[string]int64 // Nodes removed by changes to working tree.
	versions       map[int64]bool   // The previous, saved versions of the tree.
	ndb            *nodeDB
}
```

The versions map stores which versions of the IAVL are stored in nodeDB. Anytime a version `v` gets persisted, `versions[v]` is set to `true`. Anytime a version gets deleted, `versions[v]` is set to false.

### Set

### Remove

### Balance

### SaveVersion

### DeleteVersion
