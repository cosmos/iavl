# iavl/v2

The Get API must not reuse connections, this is not thread-safe and Get may be called
concurrently to both other Gets and to SaveVersion.  SaveVersion can operate within a single
connection and should scope this at the beginning of the function.

Connections must be passed as parameters down from from SaveVersion to `set` and `delete`.

Therefore both Get and Iterate must create or obtain a connection and then pass it downward.

## Checkpoints

A checkpoint writes all dirty branch nodes currently in memory since the last checkpoint to
disk. Checkpoints are distinct from shards.  One shard may contain multiple checkpoints.  

## Sharding

Each time a pruning occurs, a new shard is created.  

Options for sharding connection management:

Option (1) connection pooling.  Pool some connections which are created with ATTACH statements
for each shard.

Option (2) create a connection for each shard at query time.

Either of these cases requires a func spec like `version -> shardID` which is a static lookup
table.  For option (1) the statement is parameterized like "shard_%d.tree" into the single
connection with ATTACH'd databases.  For option (2) the connection is selected from a lookup
table, therefore no parameterization is needed on table name.  Given the unavoidable look up
required for the shardID, option (2) is simpler.

Each shard object contains a specification for creating a connection. Only one shard should be
writable at a time, the latest one.  During pruning another shard is writable but this is not
included in the shard list.

## Pruning

Parameters:

- invalidated percent: the percentage of invalidated leafs to trigger pruning.  The default is
  0.5.
- minumum keep versions: the minimum number of versions to keep.  This is a safety feature to
  prevent pruning to a version that is too recent.  The default is 100.

Pruning events only occur on checkpoint boundaries.  The prune version is the most recent
check point less than or equal to the requested prune version.

On prune the latest shard is locked (readonly) and a new shard is created.  The new shard is now
the hot shard and subsequent SaveVersion calls write leafs and branches to it.

Deletes happen by writing a new shard without orphans, updating the shard connection, then
dropping the old one.

Given the following checkpoints and current version 53:

```text
1--------11--------21--------31--------41--------51---53
```

Assuming no prior pruning and the following pruning events are the first.
There is a hot shard `tree_1` (full).

Prune to version 8: no-op.

Prune to version 13:  

Algorithm:

- Save version 53 to current hot shard.
- Create shard `tree_54`. This is the new hot shard.
- Target prune verseion = 13, so select prune version = 11.
- Return control to the caller.
- Asynchonrously do the following:
- Create shard `tree_11` (shard 1 pruned to version 13).
- `insert into tree_11 FROM select * from tree_1.branches where not orphaned`
- `insert into tree_11 FROM select * from tree_1.leafs where not orphaned`
- delete shard tree_1 from disk

Effects:

- No deletes past version 11 (prior checkpoint).
- Delete orphaned leaves from [1, 11).
- Delete orphaned branches from version 1
- Collapse tree_1 into tree_11.

Prune to version 34:

- Save version 53 to current hot shard.
- Create shard `tree_54` (full). This is the new hot shard.
- Target prune version = 34, so select prune version = 31.
- Return control to the caller.
- Asynchonrously do the following:
- Create shard `tree_31` (partial, previous shards pruned to version 34).
- `insert into tree_31 FROM select * from tree_1.branches where not orphaned JOIN 11..31`
- `insert into tree_31 FROM select * from tree_1.leafs where not orphaned JOIN 11..31`
- `insert into tree_31 FROM select * from tree_11.branches where not orphaned JOIN 21..31`
- `insert into tree_31 FROM select * from tree_11.leafs where not orphaned JOIN 21..31`
- `insert into tree_31 FROM select * from tree_21.branches where not orphaned JOIN 31`
- `insert into tree_31 FROM select * from tree_21.leafs where not orphaned JOIN 31`

Effects:

- No deletes past version 31 (prior checkpoint).
- Delete orphaned leaves from `tree_1.leaves` in [1, 11).
- Delete orphaned branches from `tree_1.branches` at version 1.
- Collapse tree_1 into tree_11.
