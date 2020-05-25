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
and must rely on some sort of state replay mechanism (e.g. Tendermint peer-to-peer gossip & state machine execution).
While this inherently is not necessarily a problem, it becomes a problem under certain contexts that
depend on a specific version existing on disk.

One such example is live upgrades. Specifically, when a live upgrade occurs in some application, typically
a new binary will be started in place of the old binary and some set of business logic will need to
be executed in order to migrate and handle old and/or invalid state to make it compatible with the
new version. Being that these operations must occur on the latest canonical state, we must ensure the
latest version is committed and flushed to disk so all operators and clients see the same view when
doing this upgrade. Hence, we need a means to manually signal to the IAVL tree that a specific
version should be flushed to disk. In addition, we also need to ensure this version is not pruned at
a later point in time.

## Decision

We propose to introduce a new API, `FlushVersion(version int64)`. This new API will attempt to manually
flush a previously committed version to disk. It will return an error if the version does not exist
in memory or if the underlying flush fails. If the version is already flushed, the method performs a
no-op and no error is returned. After the version is flushed to disk and the version is not the latest
version, we will also remove the version from the recent (in-memory) database via `DeleteVersionFromRecent`.

In order to not have a manually flushed version pruned at a later point in time
(e.g. after an application restarts), we also introduce a new type, `VersionMetadata`, that will be
flushed to disk for every version saved, regardless if the version is flushed to disk. The `VersionMetadata`
type will also provide us with added benefits around pruning model and how clients treat snapshots.

```go
type VersionStatus string

const (
  Saving   VersionStatus = "saving"   // set when a version is about to be saved
  Buffered VersionStatus = "buffered" // set when a version has been saved to volatile storage
  Saved    VersionStatus = "saved"    // set when a version has been saved to disk
  Pruned   VersionStatus = "pruned"   // set when a version has been pruned
  Deleted  VersionStatus = "deleted"  // set when a version has been explicitly deleted
)

type VersionMetadata struct {
  Version   int64         // tree version for the corresponding metadata
  Committed int64         // the UNIX timestamp of when the metadata was committed to disk
  Updated   int64         // the UNIX timestamp of when the metadata was updated (see VersionStatus)
  RootHash  []byte        // the root hash of the tree for the corresponding metadata
  Status    VersionStatus // the status of the version
  Snapshot  bool          // if this version corresponds to a version that is flushed to disk
}

func VersionMetadataKey(version int64) []byte {
  return []byte(fmt.Sprintf("metadata/%d", version))
}
```

Currently, during `MutableTree#SaveVersion` we write to both a recentDB and a snapshotDB if the
version in question is meant to be a snapshot version (i.e. by calling `isSnapshotVersion`). Afterwards,
we check if any recent version needs to be pruned from memory by calling `PruneRecentVersions`.
In addition, `isSnapshotVersion` calls are found throughout the internal API and are posed to be
problematic when the underlying pruning strategy changes
(i.e. a previous call that returned `true` for a version may now return `false`). As a result, the
internal IAVL pruning model can be hard to follow and may break invariants under certain circumstances.

The `VersionMetadata` can drastically simplify a lot of this logic. First, we propose to remove
`isSnapshotVersion` and instead set `VersionMetadata.Snapshot` and `VersionMetadata.Status = Saving`
when it's created during `MutableTree#SaveVersion`.

Now everywhere `isSnapshotVersion` is called, we can now simply refer to `VersionMetadata.Snapshot`
instead. Furthermore, whenever a version is pruned or explicitly deleted, the `VersionMetadata.Status`
will set to `Pruned` or `Deleted` respectively. Anytime existing `VersionMetadata` is updated
(e.g. when pruned or deleted), the `VersionMetadata.Updated` will reflect the timestamp of this event.

`VersionMetadata` will be managed by the `MutableTree` and will be saved to disk via `nodeDB.SnapshotDB`
during all of the following phases:

- At the start of `MutableTree#SaveVersion`
  - The fields `Version`, `Status`, and `Snapshot` will be set here.
- At the end of `MutableTree#SaveVersion`
  - The fields `Committed`, `Status`, `Updated`, and `RootHash` will be set here.
- During `MutableTree#PruneRecentVersion`
  - The fields `Updated` and `Status` will be set here.
- During `MutableTree#DeleteVersion`
  - The fields `Updated`, and `Status` will be set here.
- During `MutableTree#FlushVersion`
  - The fields `Updated`, `Status`, and `Snapshot` will be set here.

Note, at no point do we delete `VersionMetadata` for any given version. We will expose an API that
allows clients to fetch `VersionMetadata` for a given version so that any upstream business logic
may rely on this new type (e.g. deleting snapshots).

Finally, we will serialize the `VersionMetadata` type using Protocol Buffers and we will also utilize
a write-through LRU cache within `nodeDB`.

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
