# QuickBooks connector progress

Keep entries most-recent-first. Record reproducible evidence and blockers, but
never credentials, access tokens, refresh tokens, client secrets, or customer
payloads.

## 2026-07-27 — M3 Customer live acceptance complete

- Created isolated schema `workspace.quickbooks_m3`.
- Deployed serverless pipeline `quickbooks_customer_m3_cdc`
  (`82e11b44-07fa-496c-a6ef-e941b4b28097`) with packages isolated in
  `workspace.quickbooks_m3.community_connector`.
- Created refresh-first Job `quickbooks_customer_m3_cdc_with_token_refresh`
  (`412714813643415`); no M1 or M2 resource was modified.
- Bootstrap run `1123094026072585` completed successfully and materialized 29
  rows with 29 distinct IDs, zero invalid IDs, and zero missing `raw_json`
  payloads.
- No-change run `53270565059834` completed successfully, proving the committed
  checkpoint can be reused without a reset.
- Created one clearly named synthetic Customer in the QuickBooks sandbox.
  Incremental pipeline update `7a176cfc-9342-42bc-b456-6b590a58191e`
  completed and produced 30 rows, 30 distinct IDs, and exactly one synthetic
  insert.
- Applied a sparse update only to that synthetic Customer using its latest
  QuickBooks `SyncToken`. Incremental pipeline update
  `39252e5c-1359-4ca3-916e-7cab74b12b6b` completed.
- Final aggregate validation found 30 rows, 30 distinct IDs, exactly one
  updated synthetic row, zero stale versions of its prior display name, and
  zero invalid IDs. This proves the SCD Type 1 merge updated the existing row
  in place.
- Removed the temporary synthetic-ID secret and stopped the temporary SQL
  warehouse after validation. The synthetic sandbox Customer remains as an
  auditable M3 fixture.
- M3 Customer inserts and updates are accepted. Extending the pattern to the
  other five tables remains the next M3 scope.

## 2026-07-26 — M3 Customer incremental implementation complete offline

- Customers now advertise `cdc` ingestion with `last_updated_at` as the
  sequence cursor; the other five tables remain snapshot-only.
- Added versioned offsets shaped as
  `{"version": 1, "updated_through": "<UTC timestamp>"}`.
- The connector freezes an initialization-time upper bound so an AvailableNow
  run converges instead of chasing concurrent source writes.
- The first Customer read emits a complete snapshot and checkpoints the frozen
  boundary. Later reads query bounded `MetaData.LastUpdatedTime` windows.
- Added a configurable 60-second lower-bound overlap. This intentionally
  replays boundary records and relies on SCD Type 1 merges by QuickBooks `Id`
  for idempotency.
- Rejected an ID range tie-breaker after verifying that QuickBooks query
  filters do not support ordering comparisons on `Id` and do not support
  `OR`. Timestamp overlap is the lossless boundary strategy for M3.
- Added `pipeline_spec.customer_cdc.yaml` for an isolated Customer CDC pilot
  with `last_updated_at` sequencing.
- Simulator and unit coverage proves same-timestamp inclusion, bounded-window
  pagination, deterministic replay, failure replay from the unchanged start
  offset, malformed offset rejection, and missing cursor rejection.
- Offline suite result: 47 passed and 2 expected skips. Live sandbox
  insert/update validation and isolated Databricks deployment remain next.

## 2026-07-26 — M3–M6 roadmap formalized

- Defined M3 for checkpointed incremental inserts and updates.
- Defined M4 for deletion and inactive-record semantics.
- Added M5 as a required multi-tenant isolation milestone before production.
- Defined M6 for evidence-driven partitioned ingestion and performance work.

## 2026-07-26 — M2 six-table live acceptance complete

- Created isolated schema `workspace.quickbooks_m2`.
- Deployed serverless pipeline `quickbooks_six_table_m2`
  (`cade4c01-a920-4765-b372-f19b16a35cfe`) without changing the working M1
  pipeline.
- Created refresh-first Job `quickbooks_six_table_m2_with_token_refresh`
  (`983563599046304`).
- Workflow run `469908743657600` completed successfully: token refresh
  succeeded before the six-table pipeline.
- Destination integrity validation found zero duplicate, null, or blank IDs
  and zero missing `raw_json` payloads:
  - customers: 29 rows
  - vendors: 71 rows
  - accounts: 90 rows
  - items: 23 rows
  - invoices: 42 rows
  - bills: 85 rows
- Credential-free aggregate parity Job
  `quickbooks_m2_source_destination_validation` (`347443756006694`) reread
  each live QuickBooks entity without logging IDs.
- Parity run `457102424567779` proved exact source/destination ID-set equality
  for all six tables: zero source-only and zero destination-only IDs.
- A general serverless notebook could not import the preview Spark Python
  streaming data-source API used by the connector. The pipeline runtime
  supports it; the parity notebook therefore used the QuickBooks REST API
  directly and persisted Intuit's rotated refresh token before validation.
- The temporary SQL warehouse was stopped after validation.

## 2026-07-26 — rotated Intuit client secret validated

- Updated only `quickbooks_connector/client_secret` from the ignored local
  development configuration; no credential value was logged or committed.
- Serverless workflow run `119992087771059` completed successfully.
- `refresh_quickbooks_token` succeeded with the rotated client secret and
  persisted the next refresh token.
- The dependent `run_quickbooks_pipeline` task then completed successfully,
  proving that refresh and Customer ingestion still work end to end.

## 2026-07-26 — automatic token refresh validated

- Added `quickbooks_token_refresh.py` with isolated, unit-tested Intuit token
  exchange and Unity Catalog connection-option construction.
- Created Databricks secret scope `quickbooks_connector`; it stores the OAuth
  client credentials, realm, and latest rotating refresh token.
- Uploaded the refresh notebook without embedding any credential values.
- Created the serverless two-task Lakeflow Job
  `quickbooks_customer_m1_with_token_refresh` (`335040981837328`).
- Job run `799136489074724` completed successfully:
  `refresh_quickbooks_token` rotated and persisted the refresh token, updated
  `quickbooks_sandbox` with a fresh access token, and
  `run_quickbooks_pipeline` then completed successfully.
- Direct COMMUNITY refresh-token authentication is not supported by the
  backend; accepted flow values are `m2m`, `u2m`, and `u2m_per_user`.
- Direct managed U2M remains incompatible: Intuit accepts `header_only` and
  rejects `header_and_body`, while Databricks' `header_only` exchange returned
  `invalid_client`.
- Operational rule: trigger or schedule the Lakeflow Job, not the pipeline
  directly. No recurring schedule was created because an ingestion cadence
  has not yet been selected.

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
- [x] Live-source parity validation
- [x] No duplicate primary keys in any destination table
- [x] Source/destination ID parity for every table

### M3 — checkpointed incremental updates

- [x] Define and version the `LastUpdatedTime` watermark offset
- [x] Document why QuickBooks query constraints rule out an ID range tie-breaker
- [x] Implement a lossless snapshot-to-incremental handoff for Customers
- [x] Prove same-timestamp records are not skipped
- [x] Prove replay after failure is idempotent and does not advance the
      checkpoint prematurely
- [x] Validate Customer inserts and updates against live QuickBooks
- [ ] Extend the proven incremental pattern to the other five tables

### M4 — deletions and inactive records

- [ ] Define deletion versus inactivation semantics for every entity
- [ ] Implement and test `cdc_with_deletes`
- [ ] Emit stable tombstones containing the required primary key and cursor
- [ ] Prove deletion replay is idempotent
- [ ] Validate live deletion and inactivation behavior in Databricks

### M5 — multi-tenant isolation

- [ ] Enforce one QuickBooks `realm_id` per Unity Catalog connection
- [ ] Provision separate credential and refresh-token chains per tenant
- [ ] Isolate destination schemas or use `realm_id` in every shared primary key
- [ ] Isolate checkpoints by realm and table
- [ ] Prove identical QuickBooks IDs in two realms cannot collide
- [ ] Prove refreshing or revoking one tenant cannot affect another tenant
- [ ] Prove one tenant's ingestion failure does not block another tenant
- [ ] Validate Unity Catalog, secret-scope, Job, schema, and table permissions
      across tenants
- [ ] Document repeatable tenant onboarding, revocation, and data-retention
      procedures

### M6 — partitioned ingestion and performance

- [ ] Establish production-scale volume and latency targets
- [ ] Measure sequential CDC performance and QuickBooks rate-limit behavior
- [ ] Define deterministic, non-overlapping time-window partitions
- [ ] Implement shared concurrency and rate-limit controls
- [ ] Prove partition retries cannot miss or duplicate changes
- [ ] Enable partitioned Spark ingestion only when measurements show a benefit
