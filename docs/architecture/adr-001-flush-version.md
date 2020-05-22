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
state via IAVL tree(s), can no longer guarantee that when a state is committed, that it exists on disk
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

> This section describes our response to these forces. It is stated in full sentences, with active
> voice. "We will ...".
> {decision body}

## Consequences

> This section describes the resulting context, after applying the decision. All consequences should
> be listed here, not just the "positive" ones. A particular decision may have positive, negative,
> and neutral consequences, but all of them affect the team and project in the future.

### Positive

{positive consequences}

### Negative

{negative consequences}

### Neutral

{neutral consequences}

## References
