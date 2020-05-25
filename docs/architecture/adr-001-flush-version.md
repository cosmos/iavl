# ADR 001: Flush Version

## Changelog

- May 22, 2020: Initial Draft

## Status

Proposed

## Context

The IAVL library recently underwent changes to the pruning and version commitment model. Specifically,
the invariant that a version is flushed to disk when it is committed via `SaveVersion` no longer holds
true. Instead, versions are kept in memory and periodically flushed to disk and then pruned based on
the client supplied pruning strategy parameters. For more detailed information, see the
[PRUNING](../tree/PRUNING.md) document.

These changes, while drastically improving performance under certain circumstances, introduces certain
tradeoffs. Specifically, an application with a deterministic state-machine that commits and merkle-izes
state via IAVL tree(s), can no longer guarantee that a state is written to disk when it is committed
and must rely on some sort of state replay mechanism (e.g. Tendermint p2p gossip). While this inherently
is not necessarily a problem, it becomes a problem under certain contexts that depend on a specific
version existing on disk.

One such example is live upgrades. Specifically, when a live upgrade occurs in some application, typically
a new binary will be started in place of the old binary and some set of business logic will need to
be executed in order to migrate and handle old and/or invalid state to make it compatible with the
new version. Being that these operations must occur on the latest canonical state, we must ensure the
latest version is committed and flushed to disk so all operators and clients see the same view when
doing this upgrade. Hence, we need a means to manually signal to the IAVL tree, that a specific
version should be flushed to disk. In addition, we also need to ensure this version is not pruned at
a later point in time.

## Decision

We propose to introduce a new API, `FlushVersion(version int64)`. This new API will attempt to manually
flush a previously committed version to disk. It will return an error if the version does not exist
or if the underlying flush fails. If the version is already flushed, the method performs a no-op
and no error is returned. After the version is flushed to disk and the version is not the latest
version, we will also remove the version from the recent (in-memory) database via `DeleteVersionFromRecent`.

In order to not have a manually flushed version pruned at a later point in time
(e.g. after an application restarts), we also introduce a new type, `CommitMetadata`, that will be
flushed to disk for every version saved, regardless if it's flushed to disk.

```go
type PruneStrategy string

const (
  PruneStrategyNever PruneStrategy = "never"
  PruneStrategyAuto  PruneStrategy = "auto"
)

type CommitMetadata struct {
  Version   uint64
  Timestamp int64
  RootHash  []byte
  Prune     PruneStrategy
}
```

Currently, during `SaveVersion` we evaluate which versions to prune via `PruneRecentVersions` using
the `Options` provided to the tree's `nodeDB`. The change we propose is now to use `CommitMetadata.Prune`
to determine if we should actually prune a previously committed version. Under strategy `"auto"`, the
current logic will be executed. However, if a given version is to be pruned but it's strategy is `"never"`,
the version will not be pruned. During `FlushVersion`, we automatically set the strategy to `"never"`,
otherwise, during `SaveVersion` it's set to `"auto"`.

```go
func (ndb *nodeDB) PruneRecentVersions() (prunedVersions []int64, err error) {
  // ...

  err = ndb.deleteVersion(pruneVer, true, true)
  if err != nil {
    return nil, err
  }

  metadata, err := ndb.getMetadata(pruneVer)
  if err != nil {
    return nil, err
  }

  if ndb.isSnapshotVersion(pruneVer) || metadata.Prune == PruneStrategyNever {
    return nil, nil
  }

  // ...
}
```

Note, regardless of the prune strategy in a version's `CommitMetadata`, we still remove the version
from the recent (in-memory) database via `deleteVersion`.

## Consequences

### Positive

- Allows use to flush manually committed versions to disk at will
- Control over pruning of manually flushed committed versions

### Negative

- Additional storage per committed version
- One additional DB lookup during `PruneRecentVersions`
  - We can use a fixed-size LRU cache for `CommitMetadata` to nullify most of the
  IO impact here.

### Neutral

## References
