# QuickBooks connector architecture

This document captures the initial architectural decisions and the invariants
that must hold as the connector moves from snapshot ingestion to CDC.

## Accepted starting decisions

### One realm per connection

One Unity Catalog COMMUNITY connection represents one Intuit authorization and
one QuickBooks `realm_id`. The connector never multiplexes credentials or
companies inside one connection.

### OAuth stays outside the connector

The connector consumes an injected access token. It does not persist or rotate
refresh tokens and does not implement a browser callback.

The preferred production path is Unity Catalog managed U2M. The validated
fallback for Intuit interoperability is a two-task Databricks workflow:

1. A serverless notebook reads the OAuth client and refresh token from a
   Databricks secret scope.
2. It exchanges the refresh token using HTTP Basic authentication.
3. It persists Intuit's newly rotated refresh token before updating the static
   COMMUNITY connection with the short-lived access token.
4. The ingestion pipeline runs only if the refresh task succeeds.

This preserves the security boundary: Spark executors receive only an access
token, and logs never contain OAuth credentials.

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

## Implemented M3 Customer incremental design

The Customer offset is versioned and contains the committed source watermark:

```json
{
  "version": 1,
  "updated_through": "2026-07-25T10:00:00Z"
}
```

Each connector instance freezes its initialization timestamp. That timestamp is
the upper bound for the whole AvailableNow run, allowing it to terminate even
when QuickBooks is being updated concurrently.

During Customer bootstrap:

1. Capture the CDC boundary before the first snapshot request.
2. Emit the complete positional snapshot.
3. Commit the captured boundary only if Spark successfully commits the batch.
4. On the next trigger, replay the configured overlap below that boundary so
   changes racing the snapshot are included.

During Customer incremental reads:

1. Query an inclusive `MetaData.LastUpdatedTime` lower and upper bound.
2. Default to a 60-second lower-bound overlap and a one-day maximum window.
3. Paginate the entire bounded query with `STARTPOSITION` / `MAXRESULTS`.
4. Return a new `updated_through` only for the bounded upper timestamp.
5. Let Spark commit that end offset only after the batch succeeds.
6. Merge replayed records by QuickBooks `Id`, sequencing by
   `last_updated_at`, so replay is idempotent.

An ID tie-breaker is not used. QuickBooks query filters permit equality and
`IN` for `Id`, but not range comparisons, and the query language does not
support `OR`. A timestamp overlap therefore protects equal-timestamp
boundaries without relying on an unsupported `(timestamp, Id)` range cursor.

Customer metadata is `cdc`. Vendors, accounts, items, invoices, and bills
remain `snapshot` until the Customer pattern passes live acceptance.

## Required before deletion CDC

The QuickBooks CDC endpoint is still required for reliable deletion
tombstones. Before enabling `cdc_with_deletes`:

1. Subdivide any time window that reaches the 1,000-object response limit.
2. Emit updates and deletion tombstones separately.
3. Define recovery when a checkpoint is older than the 30-day CDC horizon.
4. Prove replayed deletes are idempotent.

## Open decisions

- Confirm how Databricks community OAuth surfaces Intuit's callback `realmId`.
- Re-evaluate the default overlap duration using production latency evidence.
- Decide whether `raw_json` should become `VARIANT` before public release.
- Decide which QuickBooks entities require specialized typed schemas.
- Define recovery when a checkpoint is older than the 30-day CDC horizon.
- Define inactive-versus-deleted behavior for list entities.
