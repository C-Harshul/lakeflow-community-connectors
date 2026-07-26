# QuickBooks connector architecture

This document captures the initial architectural decisions and the invariants
that must hold as the connector moves from snapshot ingestion to CDC.

## Accepted starting decisions

### One realm per connection

One Unity Catalog COMMUNITY connection represents one Intuit authorization and
one QuickBooks `realm_id`. The connector never multiplexes credentials or
companies inside one connection.

### Unity Catalog owns OAuth

The connector consumes an injected access token. It does not persist or rotate
refresh tokens and does not implement a browser callback.

### At-least-once source delivery

Uncommitted batches may be replayed. Every table uses QuickBooks `Id` as its
stable primary key so destination merges remain idempotent.

### Typed core plus lossless raw payload

The connector exposes stable identity, source metadata, common analytical
fields, and the complete source object in `raw_json`.

### Serial reads within one table

The first version does not partition positional QuickBooks query pages across
Spark executors. Parallel page reads can drift while the source changes and can
amplify API throttling.

## Required CDC design before enabling incremental metadata

The final per-table offset must be versioned and contain all restart state.
A proposed shape is:

```json
{
  "version": 1,
  "phase": "cdc",
  "committed_through": "2026-07-25T10:00:00Z"
}
```

During bootstrap:

1. Capture the CDC boundary before the first snapshot request.
2. Complete or resume the full positional snapshot.
3. Begin CDC from the captured boundary.
4. Accept replayed rows and merge by QuickBooks `Id`.

During CDC:

1. Read a bounded time window.
2. Subdivide any window that reaches the 1,000-object response limit.
3. Emit updates and deletion tombstones separately.
4. Advance `committed_through` only after the complete window is emitted.
5. Replaying the same start offset must produce equivalent records.

## Open decisions

- Confirm how Databricks community OAuth surfaces Intuit's callback `realmId`.
- Choose an overlap duration for timestamp boundary protection.
- Decide whether `raw_json` should become `VARIANT` before public release.
- Decide which QuickBooks entities require specialized typed schemas.
- Define recovery when a checkpoint is older than the 30-day CDC horizon.
- Define inactive-versus-deleted behavior for list entities.
