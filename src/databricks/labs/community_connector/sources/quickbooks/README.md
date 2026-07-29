# QuickBooks Online community connector

This directory contains the first Lakeflow-native scaffold for a QuickBooks
Online Accounting API connector.

## Current status

Implemented:

- Unity Catalog OAuth connection specification.
- One QuickBooks `realm_id` per connection.
- Six discoverable tables: customers, vendors, accounts, items, invoices,
  and bills.
- Stable typed core fields plus a lossless `raw_json` payload.
- Complete `STARTPOSITION` / `MAXRESULTS` snapshot pagination.
- Bounded retries for throttling, transient HTTP failures, and network errors.
- Checkpointed inserts and updates for all six tables using bounded
  `MetaData.LastUpdatedTime` queries.
- Versioned per-table offsets, snapshot-to-incremental handoff, and replay-safe
  timestamp overlap.
- Explicit active-and-inactive reads for Customer, Vendor, Account, and Item.
- Replay-safe Invoice and Bill hard-delete tombstones through QuickBooks CDC.
- Fail-closed protection for QuickBooks CDC's 30-day horizon and 1,000-object
  response ceiling.
- Non-null `realm_id` on every row and tombstone, with `(realm_id, id)` as the
  composite destination key.
- Version-2 checkpoints bound to realm, table, and update/delete flow.
- Refresh-time tenant binding across the Job, secret scope, and Unity Catalog
  connection.

Validation available:

- Focused unit tests for configuration, pagination, retries, HTTP failures,
  response validation, and typed normalization.
- Source simulator corpus and the repository's generic connector contract
  suite for all six tables.
- Generated single-file Spark Python data source with a tested
  `register(spark, "quickbooks")` entry point.
- Live Intuit sandbox OAuth refresh and Customer snapshot validation.
- Serverless Databricks M1 Customer pipeline with exact source/destination ID
  parity.
- Serverless Databricks M2 pipeline with exact live source/destination ID
  parity for customers, vendors, accounts, items, invoices, and bills.
- Serverless Databricks M3 Customer CDC pipeline with successful bootstrap,
  no-change replay, synthetic insert, and sparse-update acceptance.
- Isolated serverless six-table M3 CDC pipeline with successful bootstrap and
  aggregate integrity validation for every entity.
- Isolated serverless M4 pipeline with live acceptance proving Invoice/Bill
  hard deletes remove destination rows while inactive Customer/Vendor rows
  remain queryable.
- Serverless refresh-then-ingest workflow that persists Intuit refresh-token
  rotation in a Databricks secret scope before every pipeline run.
- `pipeline_spec.customer.yaml` for the M1 Customer smoke pipeline.
- `pipeline_spec.yaml` for the M2 six-table snapshot pipeline.
- `pipeline_spec.customer_cdc.yaml` for the isolated M3 Customer CDC pilot.
- `pipeline_spec.all_tables_cdc.yaml` for six-table M3 CDC ingestion.
- `pipeline_spec.all_tables_cdc_deletes.yaml` for M4 update, inactivation, and
  hard-delete ingestion.
- `pipeline_spec.multi_tenant.yaml` for the M5 tenant-isolated deployment
  pattern.
- `TENANT_OPERATIONS.md` for onboarding, permissions, revocation, migration,
  and retention.
- Interactive `community-connector setup_quickbooks` automation for OAuth,
  secret storage, tenant-bound connection creation, schema creation, source
  deployment, pipeline creation, refresh-first Job creation, and an optional
  validation run.

Not implemented or externally validated yet:

- Live synthetic insert/update acceptance for each non-Customer entity.
- Automated full reconciliation after an expired or saturated delete
  checkpoint.
- Direct Unity Catalog managed U2M. Databricks' server-side U2M exchange with
  Intuit currently returns `invalid_client`; the validated workflow in
  `quickbooks_token_refresh.py` provides automatic rotation without exposing
  long-lived credentials to the connector.

## Connection parameters

| Parameter | Required | Description |
|---|---:|---|
| `client_id` | yes | Intuit OAuth application client ID |
| `client_secret` | yes | Intuit OAuth application client secret |
| `realm_id` | yes | QuickBooks Online company ID |
| `environment` | no | `production` (default) or `sandbox` |
| `minor_version` | no | Accounting API minor version; defaults to `75` |

The Databricks control plane owns the OAuth authorization and token refresh
boundary. The connector consumes the injected `access_token` and never receives
client credentials or refresh tokens.

Until direct Unity Catalog U2M interoperates with Intuit, run ingestion through
the refresh-then-ingest Lakeflow Job. Its first task reads OAuth credentials
from a Databricks secret scope, requests a fresh access token, persists
Intuit's rotated refresh token, and updates the static COMMUNITY connection.
The pipeline task runs only after that refresh succeeds. Do not schedule the
pipeline directly when using this mode. Each tenant Job must also pass
`expected_realm_id`; refresh stops before token rotation if that value differs
from either the tenant's secret scope or the connection's SHA-256 realm-binding
comment.

Use a separate secret scope, connection, Job, pipeline, destination schema, and
checkpoint chain for every QuickBooks realm. Tables additionally use
`(realm_id, id)` so identical QuickBooks IDs cannot collide. Existing M4
pipelines have version-1 checkpoints; migrate by bootstrapping a new M5
pipeline rather than updating them in place.

## Table options

| Option | Default | Description |
|---|---:|---|
| `page_size` | `1000` | QuickBooks query page size, from 1 through 1000 |
| `incremental_overlap_seconds` | `60` | Per-table lower-bound replay overlap, from 0 through 3600 seconds |
| `max_incremental_window_seconds` | `86400` | Maximum per-table checkpoint window, from 60 through 604800 seconds |
| `delete_overlap_seconds` | `60` | Invoice/Bill delete replay overlap, from 0 through 3600 seconds |
| `initial_delete_lookback_seconds` | `300` | Invoice/Bill bootstrap delete lookback, from 0 through 86400 seconds |

## Development

All six tables start with a complete snapshot followed by bounded update
queries with independent checkpoints. Customers, vendors, accounts, and items
use `cdc` because `Active=false` must remain queryable. Invoices and bills use
`cdc_with_deletes`, which adds independent tombstone flows.

Run the offline connector suite from the repository root:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/sources/quickbooks -q
```

## Automated workspace setup

Install the CLI from this checkout and authenticate the Databricks CLI profile
for the target workspace:

```bash
cd tools/community_connector
python -m pip install -e .
cd ../..

export DATABRICKS_CONFIG_PROFILE=my-workspace-profile
community-connector setup_quickbooks
```

Alternatively, select the workspace explicitly with
`community-connector setup_quickbooks --profile my-workspace-profile`. When no
profile or `DATABRICKS_HOST` is supplied, the command prompts for the profile
instead of silently choosing a workspace. An expired login produces a concise
reauthentication command.

The command prompts for a stable tenant label, environment, destination
catalog/schema, secret scope, connection, pipeline, Job, workspace path, and
Intuit OAuth details. Every resource name can instead be supplied as an option;
inspect them with:

```bash
community-connector setup_quickbooks --help
```

Use `--dry-run` to review calculated names without starting OAuth or mutating
the workspace. By default the command opens Intuit consent, captures the
authorized `realmId`, creates or safely updates the tenant resources,
regenerates and uploads the local connector source, and starts the
refresh-then-ingest Job. Use `--manual-tokens` only when you already have an
access token, refresh token, and realm ID; use `--skip-run` to provision
without starting validation.

Re-running the same inputs updates the bound connection, deployed source,
pipeline specification, notebook, and Job instead of intentionally creating
duplicates. It refuses to reuse a connection bound to another QuickBooks
realm or to move an existing pipeline to a different schema implicitly.

The setup command does not guess organization-specific IAM grants, data
retention policy, or a production schedule. Apply those decisions after the
bootstrap using `TENANT_OPERATIONS.md`.

Once QuickBooks and Databricks credentials are current, deploy the Customer
smoke pipeline first, preserve the M2 snapshot pipeline for comparison, and
create an isolated six-table CDC pipeline:

```bash
community-connector create_pipeline quickbooks quickbooks_customer_m1 \
  --pipeline-spec \
  src/databricks/labs/community_connector/sources/quickbooks/pipeline_spec.customer.yaml

community-connector create_pipeline quickbooks quickbooks_six_table_m3_cdc \
  --pipeline-spec \
  src/databricks/labs/community_connector/sources/quickbooks/pipeline_spec.all_tables_cdc.yaml

community-connector create_pipeline quickbooks quickbooks_six_table_m4_deletes \
  --pipeline-spec \
  src/databricks/labs/community_connector/sources/quickbooks/pipeline_spec.all_tables_cdc_deletes.yaml

community-connector create_pipeline quickbooks quickbooks_tenant_m5 \
  --pipeline-spec \
  src/databricks/labs/community_connector/sources/quickbooks/pipeline_spec.multi_tenant.yaml
```
