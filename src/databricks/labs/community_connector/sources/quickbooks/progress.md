# QuickBooks connector progress

Keep entries most-recent-first. Record reproducible evidence and blockers, but
never credentials, access tokens, refresh tokens, client secrets, or customer
payloads.

## 2026-07-26 — M0 development baseline complete locally

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
  passed 24 tests with 2 expected skips.
- Scoped Ruff checks, Python compilation, and `git diff --check` pass.
- The branch is at the same upstream commit as `upstream/master` (`c964722`).
- Commit and fork push are still pending; M1 live validation is next.

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
- [ ] Branch committed and pushed to the fork

### M1 — Customer end to end

- [ ] Valid QuickBooks sandbox authorization and realm proven
- [ ] Customer source snapshot succeeds
- [ ] Customer destination table created in Databricks
- [ ] QuickBooks IDs and Databricks IDs/counts match

### M2 — six-table snapshot

- [ ] customers
- [ ] vendors
- [ ] accounts
- [ ] items
- [ ] invoices
- [ ] bills
- [ ] Simulator and live-source parity validation
- [ ] No duplicate primary keys in any destination table
- [ ] Source/destination ID parity for every table
