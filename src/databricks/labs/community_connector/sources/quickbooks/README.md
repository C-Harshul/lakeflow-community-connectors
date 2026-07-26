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
- Snapshot ingestion metadata.

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
- Serverless refresh-then-ingest workflow that persists Intuit refresh-token
  rotation in a Databricks secret scope before every pipeline run.
- `pipeline_spec.customer.yaml` for the M1 Customer smoke pipeline.
- `pipeline_spec.yaml` for the M2 six-table snapshot pipeline.

Not implemented or externally validated yet:

- Snapshot-to-CDC handoff.
- QuickBooks CDC time-window subdivision.
- `cdc_with_deletes` and deletion reads.
- Versioned incremental offsets.
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
pipeline directly when using this mode.

## Table options

| Option | Default | Description |
|---|---:|---|
| `page_size` | `1000` | QuickBooks query page size, from 1 through 1000 |

## Development

The scaffold is intentionally snapshot-only. Do not change table metadata to
`cdc` or `cdc_with_deletes` until the checkpoint and delete invariants in
`ARCHITECTURE.md` are implemented and covered by simulator and live tests.

Run the offline connector suite from the repository root:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/sources/quickbooks -q
```

Once QuickBooks and Databricks credentials are current, deploy the Customer
smoke pipeline first and then update it to the six-table spec:

```bash
community-connector create_pipeline quickbooks quickbooks_customer_m1 \
  --pipeline-spec \
  src/databricks/labs/community_connector/sources/quickbooks/pipeline_spec.customer.yaml

community-connector update_pipeline quickbooks_customer_m1 \
  --pipeline-spec \
  src/databricks/labs/community_connector/sources/quickbooks/pipeline_spec.yaml
```
