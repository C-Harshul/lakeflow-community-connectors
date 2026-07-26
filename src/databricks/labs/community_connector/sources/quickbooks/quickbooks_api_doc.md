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

## Initial object set

| Lakeflow table | QuickBooks entity | Primary key | Initial mode |
|---|---|---|---|
| customers | Customer | Id | snapshot |
| vendors | Vendor | Id | snapshot |
| accounts | Account | Id | snapshot |
| items | Item | Id | snapshot |
| invoices | Invoice | Id | snapshot |
| bills | Bill | Id | snapshot |

## Incremental design target

QuickBooks CDC returns changed entities and deletion tombstones, but:

- only the previous 30 days can be queried;
- one response can contain at most 1,000 objects;
- the connector must subdivide saturated time windows;
- the bootstrap CDC boundary must be captured before the full snapshot;
- incremental reads must be replay-safe.

These behaviors are not implemented in the initial scaffold.

## Source references

- https://developer.intuit.com/app/developer/qbo/docs/develop/authentication-and-authorization/oauth-2.0
- https://developer.intuit.com/app/developer/qbo/docs/learn/explore-the-quickbooks-online-api/data-queries
- https://developer.intuit.com/app/developer/qbo/docs/learn/explore-the-quickbooks-online-api/change-data-capture
