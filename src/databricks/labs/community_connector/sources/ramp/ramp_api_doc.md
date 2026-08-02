# Ramp Developer API research and ingestion design

Research date: 2026-08-01

Status: M1 research plus M4 ingestion decision. This document defines the
evidence and implemented constraints; it is not evidence that the API has been
exercised with live credentials.

## Decision summary

- Use OAuth 2.0 client credentials for the initial connector. A connection is
  scoped to one Ramp business and uses that business's Ramp application.
- Use `GET /developer/v1/business` to resolve the immutable Ramp business ID.
  Persist that value as `tenant_id` on every emitted row and in every offset.
- Select six MVP tables:
  - incremental upsert: `vendors`
  - snapshots: `transactions`, `reimbursements`, `users`, `departments`,
    `locations`
- Do not claim delete capture in the initial connector. Ramp exposes useful
  lifecycle states, but the researched read endpoints do not provide a
  checkpointable, complete delete feed.
- Use keyset pagination by following the complete `page.next` URL until it is
  null. The documented maximum page size is 100 for all selected list
  endpoints.
- M4 resolved the provisional classification: `transactions` and
  `reimbursements` are snapshots. Their lower-bound update filters have no
  documented update upper bound or compatible ordering, so a closed, lossless
  incremental interval cannot be proven.

## Sources and evidence policy

The canonical machine-readable source is Ramp's
[Developer API OpenAPI document](https://docs.ramp.com/openapi/developer-api.json).
It was cross-checked against Ramp's human-oriented guides and endpoint pages:

- [Developer API overview](https://docs.ramp.com/developer-api/)
- [Authorization](https://docs.ramp.com/developer-api/v1/authorization)
- [Sandbox](https://docs.ramp.com/developer-api/v1/sandbox)
- [Pagination](https://docs.ramp.com/developer-api/v1/pagination)
- [Rate limiting](https://docs.ramp.com/developer-api/v1/rate-limiting)
- [Error handling](https://docs.ramp.com/developer-api/v1/error-handling)
- [Monetary values](https://docs.ramp.com/developer-api/v1/monetary-values)
- [Webhooks](https://docs.ramp.com/developer-api/v1/webhooks)
- Endpoint references for
  [transactions](https://docs.ramp.com/developer-api/v1/api/transactions),
  [reimbursements](https://docs.ramp.com/developer-api/v1/api/reimbursements),
  [vendors](https://docs.ramp.com/developer-api/v1/api/vendors),
  [users](https://docs.ramp.com/developer-api/v1/api/users),
  [departments](https://docs.ramp.com/developer-api/v1/api/departments),
  [locations](https://docs.ramp.com/developer-api/v1/api/locations),
  [bills](https://docs.ramp.com/developer-api/v1/api/bills),
  [cards](https://docs.ramp.com/developer-api/v1/api/cards),
  [entities](https://docs.ramp.com/developer-api/v1/api/entities), and
  [business](https://docs.ramp.com/developer-api/v1/api/business).

Where documentation is incomplete or internally inconsistent, this document
records the uncertainty rather than inferring an API guarantee. Volume labels
are engineering estimates, not Ramp-published limits.

## Environments and API roots

| Environment | API root | Token endpoint | Notes |
|---|---|---|---|
| Production | `https://api.ramp.com` | `https://api.ramp.com/developer/v1/token` | Production applications and tokens only |
| Sandbox | `https://demo-api.ramp.com` | `https://demo-api.ramp.com/developer/v1/token` | Sandbox applications and tokens only; exact token exchange still requires live validation |

The environment is connector configuration, not a table option. Do not allow a
caller to supply an arbitrary base URL in production. Tokens are not portable
between sandbox and production, and each environment requires its own Ramp app.

## Authentication

### Preferred grant: OAuth 2.0 client credentials

The connector is an automated ingestion workload operating for one Ramp
business, so OAuth 2.0 client credentials is the preferred grant. Request a
token as follows:

```http
POST /developer/v1/token HTTP/1.1
Host: api.ramp.com
Authorization: Basic <base64(client_id:client_secret)>
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials&scope=<space-separated-scopes>
```

The authorization guide and the OpenAPI operation description specify HTTP
Basic client authentication and a form-encoded body. The OpenAPI request-body
media type currently says `application/json`; this is a documentation conflict.
Implementation must follow the guide/form-encoded contract and confirm it in
sandbox before record-mode validation.

The `scope` parameter must be sent explicitly. A token response contains an
opaque `access_token`, `token_type`, `expires_in`, and granted scope. Ramp's
authorization guide currently describes client-credentials tokens as valid for
10 days (864,000 seconds). Runtime behavior must use the returned `expires_in`,
not hard-code the documented duration.

Client-credentials tokens have no refresh token. Renewal means requesting a new
token with the same client credentials before expiry. If Ramp returns 401, the
client may invalidate its cached token, acquire a new one, and retry the request
once. Repeated 401 responses are terminal and must not expose the token or
client secret.

Authorization Code is Ramp's alternative for user-authorized, centrally
managed multi-tenant applications. It is not the MVP flow. A future product in
which one centrally owned Ramp integration serves unrelated customer
businesses should revisit that choice instead of sharing client credentials.

### Required MVP scopes

```text
business:read
transactions:read
reimbursements:read
vendors:read
users:read
departments:read
locations:read
```

Request the least-privilege union needed for the enabled tables. Do not request
card-vault access: the MVP does not ingest PAN, CVV, or other vault data.

### Unity Catalog connection compatibility

The repository's community connector specification supports an OAuth `m2m`
flow whose access token is supplied to the connector. That is structurally
compatible with Ramp's client-credentials grant and allows UC to own token
renewal. One detail remains unproved: Ramp requires the client credentials in
an HTTP Basic header, while the repository specification does not expose a
token-endpoint client-auth-method setting. M7 must verify the generated UC
connection performs Ramp's required exchange. If it does not, the fallback
design needs explicit review; M1 does not silently switch to connector-managed
secrets.

### Secret handling

- Never put client IDs, secrets, access tokens, or authorization headers in
  source files, test fixtures, cassette recordings, offsets, table rows, or
  logs.
- Redact URL query strings and request headers in failures.
- Use ignored local configuration only for credentialed development, then use
  UC connection properties in deployed mode.
- Rotate any secret disclosed through chat, logs, screenshots, shell history,
  or a failed fixture before use.

## Tenant identity and isolation

Call `GET /developer/v1/business` with `business:read` after authentication.
Its response contains `id`, documented as the unique business identifier. Use
that UUID as the canonical `tenant_id`.

One client-credentials connection authenticates one Ramp business. Multiple
businesses require separate connections. Ramp `entities` are legal/accounting
subdivisions within a business and are not the connector's tenant boundary.

Every row must contain `tenant_id`; primary keys are tenant-qualified even when
Ramp currently documents resource IDs as UUIDs:

```text
[tenant_id, id]
```

The offset must also contain `tenant_id`. On every read, compare the connection's
resolved business ID with the offset tenant. A mismatch is terminal and must
never continue from the other tenant's cursor.

The business lookup is control-plane metadata rather than a public table in the
MVP. It should be cached per initialized connection, with normal process-local
cache lifetime only.

## Pagination contract

Ramp list endpoints use keyset pagination and return this envelope:

```json
{
  "data": [],
  "page": {
    "next": "https://api.ramp.com/developer/v1/...?start=..."
  }
}
```

Rules for the connector:

1. Request the first page without `start`, using `page_size=100`.
2. Emit only records admitted by the table's frozen read boundary.
3. If `page.next` is non-null, follow that complete URL verbatim. Do not rebuild
   it from the last row or assume the cursor is independently meaningful.
4. Before following it, require HTTPS, the configured Ramp host, and the
   expected `/developer/v1/` path prefix. Reject cross-host continuation URLs.
5. Stop only when `page.next` is null or the connector's record cap is reached
   at a checkpoint-safe boundary.
6. Detect a repeated continuation URL, a missing `page` object, and a non-list
   `data` field as protocol failures.

`start` is documented as the ID of the last entity on the previous page. Ramp
says pagination is based on unique, ordered internal columns, but the list
endpoints generally do not document that ordering. Transactions can explicitly
sort by transaction date or amount; they cannot sort by `synced_at`. Therefore
the connector must not equate server page order with update-cursor order.

The documented `page_size` range for the researched list endpoints is 2–100,
with 20 as the default. The implementation should use 100 unless a simulator or
live endpoint demonstrates an endpoint-specific constraint.

## Rate limits, timeouts, retries, and errors

Ramp documents a default rolling limit of 200 requests per 10 seconds per
source IP and returns HTTP 429 when it is exceeded. It also documents that
server work lasting more than 60 seconds is terminated with 504.

Implementation requirements:

- Set explicit connect and read timeouts; start with a 30-second read timeout so
  the client does not wait past Ramp's server limit.
- Retry transient connection failures, 429, and 5xx responses with capped,
  jittered exponential backoff. Ramp's guide illustrates 1, 2, and 4 second
  intervals. Honor `Retry-After` when present, but do not depend on it because
  Ramp's guide does not guarantee that header.
- Bound both attempts and total sleep. A reasonable initial policy is five
  attempts with a 30-second backoff cap; finalize and test it in M2/M3.
- On 401, renew a client-credentials token and retry once. Do not loop.
- Treat 400, 403, 404, and 422 as non-retryable unless an endpoint-specific
  contract proves otherwise. A 403 should name the required scope without
  exposing request credentials.
- Validate request option names locally. Ramp notes that unknown query
  parameters may be ignored, which otherwise turns typos into silent full
  scans.

Ramp's current structured error model contains fields such as `error_code`,
`additional_info`, `notes`, and `message`, while older responses can contain
`error`. Capture only a sanitized status, error code, short message, and the
`x-trace-id` response header. Never log the full response body by default.

## Candidate table matrix

Mode terms in this document:

- **Snapshot**: repeat a complete current-state scan.
- **Incremental upsert**: initial snapshot followed by update-filtered reads;
  rows are applied by primary key. This is repository `cdc` behavior, not a
  claim that Ramp exposes an ordered change log.
- **CDC with deletes**: incremental upserts plus a complete checkpointable
  delete stream. No candidate qualifies.
- **Initially unsupported**: useful endpoint excluded until correctness or
  schema gaps are resolved.

| Table | Endpoint | Scope | Stable key | Update/filter signal | Delete/lifecycle signal | Volume estimate | M4 mode |
|---|---|---|---|---|---|---|---|
| `transactions` | `GET /developer/v1/transactions` | `transactions:read` | `id` UUID | `synced_after`; date filters; `synced_at`, `updated_at` fields | None documented | High | Snapshot |
| `reimbursements` | `GET /developer/v1/reimbursements` | `reimbursements:read` | `id` UUID | `updated_after`, `synced_after`, submitted/date filters; `updated_at` field | `state=DELETED`, but durability/completeness is undocumented | Medium | Snapshot |
| `vendors` | `GET /developer/v1/vendors` | `vendors:read` | `id` UUID | Bounded `from_updated_at` and `to_updated_at` filters | `is_active`; not a delete feed | Low/medium | Incremental upsert |
| `users` | `GET /developer/v1/users` | `users:read` | `id` UUID | Status filter only; no update cursor | Status and scheduled deactivation | Low/medium | Snapshot |
| `departments` | `GET /developer/v1/departments` | `departments:read` | `id` UUID | None | None | Low | Snapshot |
| `locations` | `GET /developer/v1/locations` | `locations:read` | `id` UUID | Entity filter only | None | Low | Snapshot |
| `bills` | `GET /developer/v1/bills` | `bills:read` | `id` UUID | Creation/date/payment filters; no general updated cursor | `is_archived`, `archived_at`; webhook exists but no pull cursor | Medium/high | Initially unsupported; snapshot is only safe read mode |
| `physical_cards` | `GET /developer/v1/cards/physical` | `cards:read` | `id` UUID | No update cursor; default response is lifecycle-filtered | Card state/suspension/termination filters need validation | Medium | Initially unsupported; snapshot only |
| `virtual_cards` | `GET /developer/v1/cards/virtual` | `cards:read` | `id` UUID | No update cursor | Request has termination filter; response schema lacks equivalent lifecycle field | Medium | Initially unsupported; snapshot only |
| `entities` | `GET /developer/v1/entities` | `entities:read` | `ramp_id` UUID | No update cursor | `is_active`; inactive/deleted-account query semantics need validation | Low | Initially unsupported; snapshot only |

All list candidates use the `data` plus `page.next` envelope and support
`page_size`/`start` keyset pagination. The candidate list is deliberately
narrower than the complete Ramp API: endpoints centered on write workflows,
secrets, card-vault data, or nested operational actions are not ingestion-table
candidates.

### Why the six-table MVP

`transactions`, `reimbursements`, and `vendors` cover the central spend flows.
`users`, `departments`, and `locations` provide compact reference dimensions
required to interpret those facts. Vendors exercise the incremental framework
path; the other five endpoints use complete snapshots where the source cannot
prove a closed update interval.

Bills are valuable but mutable after creation. Ramp's rate-limit guide warns
that a `from_created_at` strategy only discovers newly created bills and that
periodic full pulls are needed to discover later state changes. Including bills
as a recurring snapshot would add potentially large full scans to the initial
contribution. Cards have split physical/virtual schemas and default lifecycle
filters whose complete-snapshot behavior needs live proof. Entities expose both
an external `id` and a canonical `ramp_id`, along with sensitive tax metadata;
they can be added after their lifecycle and projection are specified.

## Type policy and schema evolution

Schemas must be explicit Spark `StructType` values. Do not infer them from a
response and do not use an unbounded map of raw payload fields.

| API value | Spark representation | Rule |
|---|---|---|
| UUID, enum, free text, phone/email | `StringType` | Preserve exactly; never log PII |
| ISO 8601 date-time | `TimestampType` | Normalize to UTC; preserve null |
| ISO 8601 calendar date | `DateType` | Do not apply a timezone |
| Boolean | `BooleanType` | Preserve null separately from false |
| Integer minor units | `LongType` | Preferred money representation |
| ISO 4217 currency | `StringType` | Store beside the amount |
| Legacy decimal money/rate | `DecimalType` | Never parse through float/double |
| Nested stable record | Named `StructType` or selected flattened fields | Prefer typed fields used for joins/filtering |
| Variable arrays or dynamic accounting fields | `StringType` containing canonical JSON | Deterministic serialization; document the column |

Ramp recommends the canonical money object `{amount: integer minor units,
currency_code: ISO 4217}`. Stable money objects should become paired typed
columns such as `merchant_amount_minor` and `merchant_amount_currency_code`.
Older top-level numeric amounts should use `DecimalType`, with the accompanying
currency column, until sandbox samples prove an integer-minor-unit contract.

Unknown response fields are ignored until deliberately added to the typed
schema. Missing documented optional fields become null. A documented field that
changes to an incompatible type is a schema/protocol error, not a reason to
silently stringify the complete record.

Each emitted table also adds non-Ramp metadata:

- `tenant_id: StringType` (non-null)
- `_ramp_extracted_at: TimestampType` (non-null ingestion time)

## MVP schema inventories

These inventories cover every top-level response property in the current
OpenAPI components. M2 will translate the proposed groups into exact
`StructType` definitions and will type selected nested structures. A `*_json`
projection means canonical compact JSON of that documented nested property,
not the entire source record.

### `transactions`

Primary key: `[tenant_id, id]`.

| Proposed Spark handling | OpenAPI properties |
|---|---|
| `StringType` | `id`, `card_id`, `entity_id`, `fund_id`, `limit_id`, `memo`, `merchant_category_code`, `merchant_category_code_description`, `merchant_descriptor`, `merchant_id`, `merchant_name`, `network_merchant_id`, `original_transaction_id`, `spend_program_id`, `state`, `statement_id`, `sync_status`, `trip_id`, `trip_name`, `currency_code` |
| `TimestampType` | `accounting_date`, `settlement_date`, `synced_at`, `updated_at`, `user_transaction_time` |
| `BooleanType` | `all_requirements_met_and_approved`, `card_present`, `requires_accounting_vendor_creation_to_sync` |
| `DecimalType` | `amount`, with precision fixed after sample validation |
| `LongType` | `minor_unit_conversion_rate`, `sk_category_id` |
| Typed money struct or flattened minor/currency pair | `original_transaction_amount` |
| Selected typed nested fields plus canonical JSON where needed | `card_holder`, `decline_details`, `merchant_data`, `merchant_location` |
| Canonical JSON string columns | `accounting_categories` (deprecated), `accounting_field_selections`, `attendees`, `disputes`, `line_items`, `policy_violations`, `receipts` |

Endpoint-specific requirements:

- Ramp excludes declined transactions by default. Request `state=ALL` for a
  complete table unless a future, explicit table option narrows it.
- `synced_after` is useful for polling but is not used as a Lakeflow checkpoint:
  no documented upper companion or cursor ordering closes the interval.
- The Transaction component description mentions an `updated_after` filter,
  but the list operation does not define that query parameter. Do not send it.
- Transaction sorting only covers date or amount. It does not provide
  `synced_at` ordering, so checkpoint logic cannot use response order as the
  cursor order.

### `reimbursements`

Primary key: `[tenant_id, id]`.

| Proposed Spark handling | OpenAPI properties |
|---|---|
| `StringType` | `id`, `direction`, `employee_id`, `end_location`, `entity_id`, `fund_id`, `memo`, `merchant`, `merchant_id`, `payment_batch_id`, `payment_id`, `spend_limit_id`, `start_location`, `state`, `sync_status`, `trip_id`, `type`, `user_email`, `user_full_name`, `user_id` |
| `TimestampType` | `accounting_date`, `approved_at`, `created_at`, `payment_processed_at`, `submitted_at`, `synced_at`, `updated_at` |
| `DateType` | `transaction_date` |
| `DecimalType` | `distance`, with precision fixed after sample validation |
| Typed money structs or flattened minor/currency pairs | `entity_amount`, `merchant_amount`, `payee_amount` |
| Typed nested descriptor/reference fields or canonical JSON | `trace_id` |
| Canonical JSON string columns | `accounting_field_selections`, `attendees`, `line_items`, `receipts`, `waypoints` |

Endpoint-specific requirements:

- The endpoint defaults `direction` to `BUSINESS_TO_USER`. A complete read must
  query both `BUSINESS_TO_USER` and `USER_TO_BUSINESS`, then union and dedupe by
  `[tenant_id, id]`.
- `updated_after` and `synced_after` are not used as Lakeflow checkpoints. Both
  lack a documented upper companion, so direction fan-out cannot advance a
  lossless frozen boundary.
- The state enum contains `DELETED`, but Ramp does not document that this state
  is retained forever or fully enumerable after a checkpoint. Preserve it as a
  normal status-bearing row; do not emit a Lakeflow delete.

### `vendors`

Primary key: `[tenant_id, id]`.

| Proposed Spark handling | OpenAPI properties |
|---|---|
| `StringType` | `id`, `accounting_vendor_remote_id`, `billing_frequency`, `country`, `default_entity_id`, `description`, `external_vendor_id`, `federal_tax_classification`, `merchant_id`, `name`, `name_legal`, `parent_vendor_id`, `sk_category_name`, `state`, `vendor_owner_id`, `vendor_type` |
| `TimestampType` | `created_at` |
| `BooleanType` | `is_active`, `is_deletable` |
| `LongType` | `sk_category_id` |
| Typed money structs or flattened minor/currency pairs | `total_spend_all_time`, `total_spend_last_30_days`, `total_spend_last_365_days`, `total_spend_ytd` |
| Selected typed address fields plus canonical JSON | `address`, `tax_address` |
| Typed nested policy/source fields or canonical JSON | `default_payment_method` |
| Canonical JSON string columns | `addresses`, `contacts`, `subsidiary` |

Endpoint-specific requirements:

- Request `include_draft=true` for a complete current-state table.
- Use the bounded `from_updated_at` and `to_updated_at` filters. Fully drain a
  frozen window before advancing the window offset.
- The endpoint filters on update time, but the current Vendor response schema
  does not expose an `updated_at` property. Therefore the checkpoint is the
  fully drained window boundary, not the maximum timestamp observed in rows.
- `is_active=false` is a lifecycle status, not evidence of physical deletion.

### `users`

Primary key: `[tenant_id, id]`.

| Proposed Spark handling | OpenAPI properties |
|---|---|
| `StringType` | `id`, `business_id`, `department_id`, `email`, `employee_id`, `entity_id`, `first_name`, `last_name`, `location_id`, `manager_id`, `phone`, `role`, `status` |
| `BooleanType` | `is_manager` |
| `TimestampType` | `scheduled_deactivation_date`, `scheduled_invitation_date` |
| Canonical JSON string | `custom_fields` |

The default status behavior includes active/inactive users but excludes
suspended users. For completeness, the connector should issue the documented
filter values `USER_ACTIVE`, `USER_DRAFT`, `USER_INACTIVE`, and
`USER_SUSPENDED`, union the results, and dedupe by primary key. The response
enum includes additional invitation lifecycle values not all exposed by the
filter enum; M3 simulator cases and M7 sandbox data must confirm complete
coverage. User fields are PII and must never appear in routine logs.

No update timestamp or update filter is documented, so this is a snapshot.

### `departments`

Primary key: `[tenant_id, id]`.

| Proposed Spark handling | OpenAPI properties |
|---|---|
| `StringType` | `id`, `name` |

There is no update or delete cursor. Read all pages as a snapshot.

### `locations`

Primary key: `[tenant_id, id]`.

| Proposed Spark handling | OpenAPI properties |
|---|---|
| `StringType` | `id`, `name`, `entity_id` |

There is no update or delete cursor. Read all pages as a snapshot. Do not apply
an entity filter for the default complete table.

## Incremental offset and admission-control requirements

The M4 vendor offset is a versioned JSON envelope with table, tenant,
environment, flow, phase, cursor, and frozen upper bound. Active page groups
also carry the complete opaque continuation URL, page count, and—during an
incremental window—the original window bounds and committed cursor.

1. Every offset is versioned and contains `tenant_id`, table name, environment,
   mode, and the frozen read boundary. Reject malformed, cross-table,
   cross-environment, and cross-tenant offsets.
2. Initialization must freeze its boundary before page one. A retry resumes the
   same logical window rather than selecting a newer wall-clock time.
3. A returned end offset may advance only past records actually emitted or past
   a server window that has been fully drained.
4. `max_records_per_batch` is admission control, not a license to discard the
   remainder of a fetched page. If an opaque `page.next` only resumes after the
   entire page, the connector must either retain a deterministic intra-page
   checkpoint or stop before requesting a page it cannot safely admit.
5. Cursor comparisons use parsed UTC timestamps. Vendor rows do not expose the
   filtered update time, so advancement is based only on a fully drained server
   window, never on response order or a row maximum.
6. Incremental filters that are exclusive require a deliberate overlap and
   primary-key deduplication. Never add an undocumented epsilon to timestamps.
7. Available-now termination is represented by returning an end offset equal to
   the next start offset only after the frozen target has been completely
   drained.

### Per-table cursor implications

| Table | Frozen-boundary implication | Current conclusion |
|---|---|---|
| `vendors` | Server accepts exclusive `from_updated_at` and inclusive `to_updated_at`; the connector fully drains each `(from, to]` window before advancing | Incremental upsert |
| `transactions` | `synced_after` has no documented upper companion and results are not ordered by `synced_at` | Snapshot; M4 rejected unsafe CDC |
| `reimbursements` | `updated_after`/`synced_after` have no documented upper update bound | Snapshot; M4 rejected unsafe CDC |

The table API's date filters are not substitutes for an update upper bound:
records can change after their transaction, submission, or creation dates.

## Delete semantics

| Evidence | Treatment |
|---|---|
| Reimbursement `state=DELETED` | Emit as an upserted status row; no delete event |
| Vendor/user/entity active or lifecycle fields | Preserve current status; no delete event |
| Bill archive fields and bill archive webhook | Not an MVP table; webhook is not a pull/checkpoint source |
| Card terminated/suspended state | Not an MVP table; preserve lifecycle when later supported |
| Ramp webhooks generally | Useful future notification mechanism, but delivery to an external receiver is not a Lakeflow read cursor and does not prove historical completeness |

No MVP table is `cdc_with_deletes`, and the connector must not override
`read_table_deletes`. A future delete implementation needs a documented,
replayable list endpoint or a separately persisted webhook event log with
retention, ordering, deduplication, and checkpoint guarantees.

## Per-table request plan

| Table | Complete snapshot requests | Incremental request |
|---|---|---|
| `transactions` | `state=ALL`, paginate to null | Not applicable |
| `reimbursements` | Run once per documented direction, paginate each, union/dedupe | Not applicable |
| `vendors` | `include_draft=true`, freeze `to_updated_at` before page one, paginate to null | Drain successive frozen `(from_updated_at, to_updated_at]` windows; overlap only the first window in a trigger |
| `users` | Run once per documented status, paginate each, union/dedupe | Not applicable |
| `departments` | Paginate to null | Not applicable |
| `locations` | Paginate to null without entity restriction | Not applicable |

All tables accept validated `page_size`. Vendors additionally accept
`max_records_per_batch`, `window_seconds`, and `lookback_seconds`. The minimum
batch cap is two because Ramp's minimum page size is two. The connector lowers
the requested page size to the cap, admits only whole pages, and stops before
requesting a page that may not fit. Business filters and arbitrary start times
are not exposed because they change completeness.

## Known gaps and validation risks

1. No live Ramp call was made in M1. Sandbox scopes, example nullability,
   pagination URLs, and actual response types remain to be recorded with a
   rotated credential in M7.
2. UC-managed M2M compatibility with Ramp's HTTP Basic token exchange is not
   proven.
3. Ramp's OpenAPI media type for the token request conflicts with its guide and
   operation description.
4. The stability of keyset pagination under concurrent inserts/updates and the
   exact internal sort columns are undocumented.
5. `transactions` has `synced_after` but no update/sync upper-bound filter. The
   schema description's mention of `updated_after` is not present in the list
   operation. M4 therefore keeps it snapshot-only.
6. `reimbursements` has update/sync lower bounds but no corresponding upper
   bound. Completeness of its `DELETED` state is not documented. M4 therefore
   keeps it snapshot-only.
7. `vendors` supports bounded update filters but omits `updated_at` from its
   response schema.
8. User default status behavior is incomplete, and the response/filter enums
   are not identical. Multi-query completeness needs testing.
9. Ramp does not guarantee a `Retry-After` header in the researched rate-limit
   guide.
10. Optional/null field behavior, decimal precision, money-object variants,
    and nested schemas need captured sandbox examples before schemas are final.
11. Sandbox and production application provisioning/scopes are separate; an
    administrator may need to grant every selected read scope.
12. A per-business M2M connector does not solve centrally managed public
    multi-tenant authorization. That would require a separate Authorization
    Code design.

## Research log

| Date | Activity | Result |
|---|---|---|
| 2026-08-01 | Read Ramp overview, auth, sandbox, pagination, rate, error, money, and webhook guides | Established environment, M2M, page, rate/error, type, and delete constraints |
| 2026-08-01 | Downloaded and parsed the official OpenAPI document locally | Enumerated endpoint parameters, scopes, schemas, enums, and envelopes |
| 2026-08-01 | Cross-checked ten candidate endpoint reference pages | Selected six MVP tables and recorded exclusions |
| 2026-08-01 | Compared auth needs with the repository connector specification | UC M2M is structurally suitable; Basic-auth exchange remains a live validation item |
| 2026-08-01 | Reviewed repository API research template and reference connector documents | Aligned terminology, required evidence, and milestone boundary |
| 2026-08-01 | Resolved M4 cursor proof obligations | Kept bounded vendor CDC; downgraded transactions and reimbursements to snapshots |

## M1 completion checklist

- [x] Production and sandbox API roots documented.
- [x] Preferred grant, token endpoint, scopes, lifetime, and renewal documented.
- [x] Tenant identity and one-connection-per-business model documented.
- [x] UC M2M fit and its unverified client-auth detail documented.
- [x] Pagination, page size, ordering limitation, rate limit, timeout, retries,
      and safe errors documented.
- [x] Ten candidate tables classified.
- [x] Six MVP tables selected with stable keys, scopes, typed field inventories,
      completeness filters, and ingestion modes.
- [x] No unsupported delete guarantee claimed.
- [x] Incremental cursor and batch-admission proof obligations carried into M4.
- [x] Known documentation conflicts and live-validation gaps recorded.

## M4 implementation checklist

- [x] Unsafe transaction and reimbursement CDC claims removed.
- [x] Vendor initialization freezes `to_updated_at` before page one.
- [x] Subsequent runs use closed `(from_updated_at, to_updated_at]` windows.
- [x] Lookback is applied once per trigger, not once per window.
- [x] Full `page.next` continuations and active window bounds are checkpointed.
- [x] Only fully drained windows advance the committed cursor.
- [x] Whole-page admission control never truncates or discards a fetched page.
- [x] Available-now equality termination and invalid-offset rejection are tested.
