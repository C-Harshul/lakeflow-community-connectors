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

Not implemented yet:

- Simulator corpus and generic connector tests.
- Live Intuit sandbox validation.
- Snapshot-to-CDC handoff.
- QuickBooks CDC time-window subdivision.
- `cdc_with_deletes` and deletion reads.
- Versioned incremental offsets.
- Generated single-file deployment artifact.
- Databricks workspace pipeline validation.

## Connection parameters

| Parameter | Required | Description |
|---|---:|---|
| `client_id` | yes | Intuit OAuth application client ID |
| `client_secret` | yes | Intuit OAuth application client secret |
| `realm_id` | yes | QuickBooks Online company ID |
| `environment` | no | `production` (default) or `sandbox` |
| `minor_version` | no | Accounting API minor version; defaults to `75` |

The Unity Catalog connection owns the OAuth authorization and token refresh
flow. The connector consumes the injected `access_token` and does not store
refresh tokens.

## Table options

| Option | Default | Description |
|---|---:|---|
| `page_size` | `1000` | QuickBooks query page size, from 1 through 1000 |

## Development

The scaffold is intentionally snapshot-only. Do not change table metadata to
`cdc` or `cdc_with_deletes` until the checkpoint and delete invariants in
`ARCHITECTURE.md` are implemented and covered by simulator and live tests.
