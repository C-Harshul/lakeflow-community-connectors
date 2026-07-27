# QuickBooks Online Accounting API research notes

## Authentication

- OAuth 2.0 authorization-code flow.
- Accounting scope: `com.intuit.quickbooks.accounting`.
- Authorization endpoint:
  `https://appcenter.intuit.com/connect/oauth2`.
- Token endpoint:
  `https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer`.
- API calls require both a bearer access token and a QuickBooks company
  `realmId`.

The connector delegates token acquisition and refresh to the Unity Catalog
COMMUNITY connection. The connector treats the runtime `access_token` as
opaque.

## Query API

Entities are read with the QuickBooks SQL-like query endpoint:

```text
GET /v3/company/{realmId}/query
```

Queries use positional pagination:

```sql
SELECT * FROM Customer STARTPOSITION 1 MAXRESULTS 1000
```

The maximum response size is 1,000 records.

Customer incremental queries use inclusive time bounds:

```sql
SELECT * FROM Customer
WHERE MetaData.LastUpdatedTime >= '2026-07-25T09:59:00Z'
  AND MetaData.LastUpdatedTime <= '2026-07-26T10:00:00Z'
STARTPOSITION 1 MAXRESULTS 1000
```

QuickBooks supports `AND` but not `OR`. Its `Id` filter supports equality and
`IN`, not ordering comparisons. The M3 cursor therefore uses a timestamp
watermark with an overlap rather than an unsupported `(timestamp, Id)` range
tie-breaker.

## Initial object set

| Lakeflow table | QuickBooks entity | Primary key | Initial mode |
|---|---|---|---|
| customers | Customer | Id | cdc (inserts and updates) |
| vendors | Vendor | Id | cdc (inserts and updates) |
| accounts | Account | Id | cdc (inserts and updates) |
| items | Item | Id | cdc (inserts and updates) |
| invoices | Invoice | Id | cdc (inserts and updates) |
| bills | Bill | Id | cdc (inserts and updates) |

## Incremental status

Inserts and updates for all six tables are implemented with an independent
versioned `updated_through` offset, a frozen per-run upper bound, bounded
update windows, and replay overlap. Each table's initial batch is a complete
snapshot.

The QuickBooks CDC endpoint remains the design target for deletions:

QuickBooks CDC returns changed entities and deletion tombstones, but:

- only the previous 30 days can be queried;
- one response can contain at most 1,000 objects;
- the connector must subdivide saturated time windows;
- deletion reads must be replay-safe.

## Source references

- https://developer.intuit.com/app/developer/qbo/docs/develop/authentication-and-authorization/oauth-2.0
- https://developer.intuit.com/app/developer/qbo/docs/learn/explore-the-quickbooks-online-api/data-queries
- https://developer.intuit.com/app/developer/qbo/docs/learn/explore-the-quickbooks-online-api/change-data-capture
