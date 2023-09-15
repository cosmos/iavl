# Design doom thoughts

## modes of operation

IAVL (SC) persistence operating modes:

- `latest` - only the latest version is persisted at checkpoints. orphans are collected in memory and
  removed at checkpoints where `orphan.version <= lastCheckpointVersion`. this has the effect of slowly
  collapsing and shrinking past checkpoints. it uses the least amount of disk space. highest throughput,
  probably useful for validators or query nodes which don't need to maintain a full history in SC. similar
  to `pruning=everything` but an order of magnitude faster.

- `snapshot` - each checkpoint materializes a full snapshot to disk, that is, all nodes where
  `node.version > lastCheckpointVersion`. checkpoint interval will always be honored, but the soft memory
  ceiling may cause additional checkpoints to occur. a pruning process should run periodically to collapse
  checkpoints into the desired cadence. a good balance between disk space, throughput, and replayability
  when used with SS. proofs can be generated for any _v_ where there is a snapshot version _v' <= v_ by
  replaying the changelog in SS.

- `head` - the last _n_ states are persisted to disk. this is a the legacy pruning behavior of IAVL. a
  pruning process manages removing old states from disk. proofs just fetch tree nodes directly from disk by
  loading a root node and iterating a tree with one disk read per node.  same behavior as `pruning=default`

- `full` - all nodes are persisted to disk at every version. this is the legacy archive node behavior
  `pruning=nothing` of IAVL.
