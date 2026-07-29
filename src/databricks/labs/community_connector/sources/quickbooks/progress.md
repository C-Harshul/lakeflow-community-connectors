# QuickBooks connector progress

Keep entries most-recent-first. Record reproducible evidence and blockers, but
never credentials, access tokens, refresh tokens, client secrets, or customer
payloads.

## 2026-07-29 — M5.5 repeatable workspace setup CLI implemented

- Added `community-connector setup_quickbooks`, an interactive setup workflow
  for a new workspace. Resource names are supplied by options or prompts, with
  tenant/environment-derived defaults rather than fixed deployment names.
- Added a reusable provisioning module for setup-plan validation, Intuit OAuth
  authorization-code exchange, realm capture, connection options, six-table
  pipeline specification, tenant binding, refresh-first Job settings, and
  idempotent Databricks resource operations.
- The command can create or update the dedicated secret scope, realm-bound
  COMMUNITY connection, destination schema, local-source Lakeflow pipeline,
  refresh notebook, and dependent Job, then optionally start a validation run.
- Added fail-closed reuse checks: a connection bound to another realm and a
  pipeline targeting another destination are not silently repurposed.
- Added `--dry-run`, `--manual-tokens`, `--no-browser`, `--skip-run`, and
  non-interactive naming options. OAuth credentials and tokens are hidden and
  never written to temporary pipeline configuration.
- Added explicit `--profile` selection and an interactive profile prompt so
  setup cannot silently target `DEFAULT`; expired Databricks authentication
  now returns a concise `databricks auth login` recovery command without an
  SDK traceback.
- The connector's generated single-file Spark source is rebuilt before upload,
  preventing a workspace deployment from accidentally using stale code.
- Added focused tests for dynamic naming, composite keys/delete options, OAuth
  callback realm capture, sanitized token failures, tenant binding, secret
  storage, connection isolation, Job updates, and mutation-free dry runs.
- Verification: 258 CLI tests passed; the QuickBooks connector suite passed
  78 tests with 1 expected skip; focused Ruff and `git diff --check` passed.
- Updated the connector README and tenant runbook. Workspace-specific IAM
  grants, retention policy, recurring scheduling, and acceptance in a second
  live QuickBooks realm remain deliberate operator/external steps.

## 2026-07-28 — M5 tenant isolation implemented and live-tested

- Added non-null `realm_id` to every table schema, normalized row, and delete
  tombstone. Lakeflow metadata and all pipeline specifications now use
  `(realm_id, id)` as the composite primary key.
- Upgraded offsets to version 2. Every update/delete checkpoint is bound to
  `realm_id`, table name, flow name, and `updated_through`; cross-realm,
  cross-table, and cross-flow state is rejected before an API request.
- Added refresh-time tenant binding. Each Job supplies `expected_realm_id`,
  which must match the secret scope and the connection's non-reversible
  `quickbooks-realm-sha256:` comment before token rotation.
- Added offline proof that identical QuickBooks IDs in two realms have
  distinct destination keys, a failed tenant reader does not affect another
  connector instance, and a rejected refresh binding does not affect another
  tenant binding.
- Added `pipeline_spec.multi_tenant.yaml` and `TENANT_OPERATIONS.md`, covering
  separate per-realm secret scopes, connections, Jobs, pipelines, schemas,
  permissions, onboarding, revocation, version-1 migration, and retention.
- Offline QuickBooks suite: 78 passed and 1 expected skip. Focused Ruff,
  pipeline-spec validation, and `git diff --check` pass.
- Created isolated schema `workspace.quickbooks_m5_tenant_isolation`,
  serverless pipeline `quickbooks_six_table_m5_tenant_isolation`
  (`7ee521a1-9065-4567-aea2-424e19275757`), and refresh-first Job
  `quickbooks_six_table_m5_tenant_isolation_with_token_refresh`
  (`920407647339047`).
- Bootstrap Job run `441799907318474` and pipeline update
  `7899e956-f8da-49f2-a1d0-6e58a2107f3e` succeeded. The tenant-binding refresh
  task completed before ingestion.
- Checkpoint-resume Job run `953208668298466` and pipeline update
  `a9cb7780-e4ef-4438-bb69-9ab9fb9d3a68` also succeeded, proving the deployed
  version-2 checkpoint chain resumes normally.
- Both post-run validations found row counts equal to distinct composite-key
  counts and zero missing or unexpected realms:
  - accounts: 91
  - bills: 85
  - customers: 33
  - invoices: 42
  - items: 23
  - vendors: 74
- A second live QuickBooks realm and second tenant principal are not available
  in this workspace. Cross-realm collision, refresh rejection, and independent
  failure behavior are covered offline; cross-tenant Unity Catalog permission
  acceptance remains an external validation item.
- The temporary SQL warehouse was stopped after validation. M1 through M4
  pipelines and destination schemas were not modified.

## 2026-07-27 — M4 deletions and inactive records accepted

- Defined QuickBooks removal semantics per entity. Customer, Vendor, Account,
  and Item remain rows when `Active=false`; Invoice and Bill hard deletes
  become Lakeflow tombstones.
- List snapshots and incremental reads now explicitly request
  `Active IN (true, false)` so QuickBooks' active-only query default cannot
  hide inactive rows.
- Invoice and Bill advertise `cdc_with_deletes`. Their independent delete
  flows call the QuickBooks CDC endpoint, filter `status=Deleted`, and emit
  schema-complete tombstones with `id`, `last_updated_at`, and `raw_json`.
- Delete reads use a five-minute bootstrap lookback and 60-second replay
  overlap. They fail without advancing the checkpoint if it is outside the
  30-day CDC horizon or if a response reaches QuickBooks' 1,000-object limit.
- Added simulator CDC fixtures plus tests for response parsing, tombstone
  shape, update filtering, deterministic replay, stale checkpoints, saturated
  responses, and list-entity delete rejection.
- Offline QuickBooks suite: 67 passed and 1 expected skip. Focused Ruff checks
  and `git diff --check` pass.
- Created isolated schema `workspace.quickbooks_m4_deletes`, serverless
  pipeline `quickbooks_six_table_m4_deletes`
  (`8cb0c47b-5eb0-413a-a574-944334a606d4`), and refresh-first Job
  `quickbooks_six_table_m4_deletes_with_token_refresh`
  (`357743515706792`).
- Bootstrap Job run `289803839255465` and pipeline update
  `9a758e16-2efd-4098-a119-18d7891b0186` succeeded. All six tables had matching
  row/distinct-ID counts and zero missing cursors or raw payloads.
- Live acceptance Job run `957626230303714` succeeded end to end:
  - created a clearly marked synthetic Customer, Vendor, Invoice, and Bill;
  - ingested them in update `eaf9fa50-bb56-4ef2-9aff-43ea5cf307ca`;
  - hard-deleted the Invoice and Bill and inactivated the Customer and Vendor;
  - ingested removal changes in update
    `12fe1518-ea0b-47b9-b879-989a4edee6de`.
- Acceptance validation found a retained inactive Customer and Vendor, zero
  matching Invoices, and zero matching Bills. This proves list inactivation
  remains queryable while transaction tombstones remove destination rows.
- A first fixture attempt selected a non-postable Item and stopped before
  pipeline execution. Its partial synthetic Customer/Vendor rows were
  subsequently inactivated by cleanup run `340358645897473` and pipeline
  update `6f820790-7474-4090-a655-2a83e94679f3`. Final cleanup validation
  found all three synthetic Customers and all three synthetic Vendors
  inactive, no active synthetic list rows, and no synthetic transactions.

## 2026-07-27 — M3 extended to all six tables

- Generalized the versioned snapshot-to-incremental handoff from Customers to
  vendors, accounts, items, invoices, and bills without duplicating per-entity
  checkpoint code.
- All six tables now advertise `cdc`, sequence SCD Type 1 merges by
  `last_updated_at`, and maintain independent versioned offsets.
- Added simulator and unit coverage proving every entity uses its own
  QuickBooks query name, includes equal-timestamp boundaries, rejects missing
  `LastUpdatedTime`, and preserves bounded-window replay semantics.
- Offline suite result: 58 passed and 2 expected skips. The six-table pipeline
  specification validates with no warnings, and rebuilt wheels contain no
  bytecode cache artifacts.
- Created isolated schema `workspace.quickbooks_m3_all`.
- Deployed serverless pipeline `quickbooks_six_table_m3_cdc`
  (`a3474c64-a29c-4b3b-a422-e90b20b2a67d`) and refresh-first Job
  `quickbooks_six_table_m3_cdc_with_token_refresh` (`523934347063516`).
- Bootstrap run `981658094318392` and pipeline update
  `938f3d1d-e5ab-4cae-9ada-907d6eafeab1` completed all six flows.
- Bootstrap aggregate validation found matching row and distinct-ID counts,
  with zero invalid IDs, missing cursors, or missing raw payloads:
  - customers: 30
  - vendors: 71
  - accounts: 90
  - items: 23
  - invoices: 42
  - bills: 85
- No-change replay run `330833443763570` and pipeline update
  `69afeca1-a4a0-4129-b451-880fa3d6c700` completed all six flows. Repeating the
  aggregate validation produced identical results, proving all six
  checkpoints resume idempotently.
- Customer already has live synthetic insert/update acceptance. Additional
  per-entity synthetic mutations remain an optional deeper acceptance step;
  all non-Customer entities have completed live bounded-query bootstrap and
  checkpoint replay.
- The SQL warehouse was stopped after validation. M2 and the Customer-only M3
  resources were not modified.

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
- [x] Extend the proven incremental pattern to the other five tables

### M4 — deletions and inactive records

- [x] Define deletion versus inactivation semantics for every entity
- [x] Implement and test `cdc_with_deletes`
- [x] Emit stable tombstones containing the required primary key and cursor
- [x] Prove deletion replay is idempotent
- [x] Validate live deletion and inactivation behavior in Databricks

### M5 — multi-tenant isolation

- [x] Enforce one QuickBooks `realm_id` per Unity Catalog connection
- [x] Provision separate credential and refresh-token chains per tenant
- [x] Isolate destination schemas or use `realm_id` in every shared primary key
- [x] Isolate checkpoints by realm and table
- [x] Prove identical QuickBooks IDs in two realms cannot collide
- [x] Prove refreshing or revoking one tenant cannot affect another tenant
- [x] Prove one tenant's ingestion failure does not block another tenant
- [ ] Validate Unity Catalog, secret-scope, Job, schema, and table permissions
      across two live tenant principals
- [x] Document repeatable tenant onboarding, revocation, and data-retention
      procedures

### M6 — partitioned ingestion and performance

- [ ] Establish production-scale volume and latency targets
- [ ] Measure sequential CDC performance and QuickBooks rate-limit behavior
- [ ] Define deterministic, non-overlapping time-window partitions
- [ ] Implement shared concurrency and rate-limit controls
- [ ] Prove partition retries cannot miss or duplicate changes
- [ ] Enable partitioned Spark ingestion only when measurements show a benefit
