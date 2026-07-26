# QuickBooks connector progress

Keep entries most-recent-first. Record reproducible evidence and blockers, but
never credentials, access tokens, refresh tokens, client secrets, or customer
payloads.

## 2026-07-26 — M1 Databricks pipeline validated end to end

- Authenticated Databricks profile `numina-quickbooks` against workspace
  `7474654957615251`.
- Created isolated schema `workspace.quickbooks_m1`.
- Created and ran the serverless pipeline `quickbooks_customer_m1`
  (`b13a2a15-ec62-45b4-8493-256729be0336`); update
  `6a9985b5-4fbe-462c-b251-3eedeec5025c` completed successfully.
- Uploaded the framework and QuickBooks wheels to the managed
  `workspace.quickbooks_m1.community_connector` volume.
- Materialized `workspace.quickbooks_m1.customers` with the expected 14-column
  schema.
- Destination validation returned 29 rows, 29 distinct IDs, and zero empty
  `raw_json` payloads.
- A direct, non-logging set comparison proved that the 29 live QuickBooks
  Customer IDs exactly match the 29 Databricks destination IDs.
- M1 used a temporary Unity Catalog static connection containing only a
  short-lived access token. It contains no client secret or refresh token.
- Managed Unity Catalog U2M remains blocked: the first Databricks token
  exchange timed out and the retry reached Intuit but returned
  `invalid_client`. The generic CLI now supports provider-specific loopback
  redirect host/path controls and always supplies Databricks' required U2M
  verifier; QuickBooks also declares Intuit's `header_only` credential
  exchange.

## 2026-07-26 — M1 live QuickBooks source validated

- Completed a new Intuit OAuth authorization against a QuickBooks sandbox.
- Exchanged the saved refresh token for a short-lived access token without
  persisting or logging the access token.
- Ran `QuickBooksLakeflowConnect` against the live Customer query endpoint.
- The complete Customer snapshot returned 29 records with unique IDs and a
  non-empty lossless `raw_json` payload for every record.
- The remaining M1 work is Databricks-side: create the Unity Catalog COMMUNITY
  connection, deploy the Customer pipeline, and compare the resulting
  destination IDs/count against this source snapshot.

## 2026-07-26 — deployment Step 1 complete

- Generated the deployable single-file Spark Python data source at
  `_generated_quickbooks_python_source.py`.
- Regenerated it twice with the same SHA-256 digest:
  `1ace9555554867f3946d6c8031c4695221a3c662d2f0b193a8fd01871c9bd985`.
- Verified that `register(spark, "quickbooks")` registers `LakeflowSource`,
  constructs `QuickBooksLakeflowConnect` from Spark options, discovers all six
  tables, and returns the Customer schema.
- Added a regression test for the generated module's legacy Spark registration
  path.
- The QuickBooks and Spark registry suite passes 36 tests with 2 expected
  skips; scoped Ruff, formatting, compilation, and diff checks pass.
- Rebuilt and inspected the connector wheel; it contains the generated Python
  source, connector specification, and both M1/M2 pipeline specifications.
- Live M1 acceptance still requires renewed QuickBooks sandbox authorization
  and valid Databricks workspace authentication.

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

- [x] Valid QuickBooks sandbox authorization and realm proven
- [x] Customer source snapshot succeeds
- [x] Customer destination table created in Databricks
- [x] QuickBooks IDs and Databricks IDs/counts match

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
