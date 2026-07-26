# QuickBooks connector progress

Keep entries most-recent-first. Record reproducible evidence and blockers, but
never credentials, access tokens, refresh tokens, client secrets, or customer
payloads.

## 2026-07-26 — M2 offline implementation complete; live acceptance blocked

- Added M1 Customer-only and M2 six-table pipeline specifications; both pass
  the repository pipeline-spec validator.
- Added simulator assertions for unique source IDs, lossless raw-ID parity,
  and retention of inactive customers, vendors, accounts, and items.
- The scoped suite now passes 26 tests with 2 expected skips.
- Built and inspected
  `lakeflow_community_connectors_quickbooks-0.1.0-py3-none-any.whl`; it contains
  connector code, schemas, connection spec, documentation, and pipeline specs.
- The QuickBooks connection spec passes CLI parsing and option validation as a
  `u2m` OAuth connector with `page_size` as its source option allowlist.
- Live source attempt: the saved QuickBooks access token returned HTTP 401.
  Forced refresh returned HTTP 400, so Intuit reauthorization is required.
  A new authorization flow is open and waiting for the user-provided one-time
  verification code.
- Databricks identity attempt: the saved project PAT was rejected as invalid.
  A new PAT or another valid Databricks authentication method is required
  before creating the COMMUNITY connection, uploading wheels, or running the
  M1/M2 pipeline.

## 2026-07-26 — M0 development baseline complete and pushed

- Created a Python 3.13 development environment with PySpark 4.2.0.
- Added focused coverage for missing credentials, invalid environments,
  table discovery, specialized schemas, multi-page reads, an exactly-full
  terminal page, 401/403 failures, `429 Retry-After`, transient and network
  retry exhaustion, invalid JSON, decimal/timestamp/date conversion, and
  transaction-line preservation.
- Added a source simulator with entity-aware QuickBooks query parsing and
  representative corpus data for all six tables, including inactive list
  entities and multi-page fixtures.
- Evidence:
  `PYTHONPATH=src .venv/bin/python -m pytest tests/unit/sources/quickbooks -q`
  initially passed 24 tests with 2 expected skips; the M2 integrity additions
  raised this to 26 passing tests.
- Scoped Ruff checks, Python compilation, and `git diff --check` pass.
- The branch is at the same upstream commit as `upstream/master` (`c964722`).
- Commit `045d7cd` was pushed to
  `origin/feat/quickbooks-connector`; M1 live validation is next.

## 2026-07-26 — M0–M2 implementation started

- Fork: `C-Harshul/lakeflow-community-connectors`
- Branch: `feat/quickbooks-connector`
- M0 scaffold exists with a Unity Catalog OAuth specification, one-realm-per-
  connection model, bounded HTTP retries, positional snapshot pagination, and
  six discoverable tables.
- Replaced the initial shared schema with stable, entity-specific schemas for
  customers, vendors, accounts, items, invoices, and bills.
- Development dependencies, simulator fixtures, automated contract tests,
  live QuickBooks validation, Databricks pipeline proof, commits, and fork push
  are still in progress.

## Milestone acceptance checklist

### M0 — development baseline

- [x] Connector scaffold and architecture decisions
- [x] Six-table discovery
- [x] Entity-specific schemas
- [x] Unit tests for configuration, pagination, HTTP failures, retries, and normalization
- [x] Repository generic connector tests pass
- [x] Branch committed and pushed to the fork

### M1 — Customer end to end

- [ ] Valid QuickBooks sandbox authorization and realm proven
- [ ] Customer source snapshot succeeds
- [ ] Customer destination table created in Databricks
- [ ] QuickBooks IDs and Databricks IDs/counts match

### M2 — six-table snapshot

- [x] customers implementation and simulator coverage
- [x] vendors implementation and simulator coverage
- [x] accounts implementation and simulator coverage
- [x] items implementation and simulator coverage
- [x] invoices implementation and simulator coverage
- [x] bills implementation and simulator coverage
- [ ] Live-source parity validation
- [ ] No duplicate primary keys in any destination table
- [ ] Source/destination ID parity for every table
