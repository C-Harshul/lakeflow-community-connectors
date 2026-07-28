# QuickBooks tenant operations

This runbook defines the isolation boundary for onboarding, operating, and
revoking QuickBooks Online companies.

## Isolation unit

Treat one QuickBooks `realm_id` as one tenant. Each tenant must have its own:

- Databricks secret scope containing `client_id`, `client_secret`,
  `refresh_token`, and `realm_id`;
- Unity Catalog COMMUNITY connection;
- refresh-then-ingest Job and pipeline;
- destination schema in production.

The destination schema is the operational isolation boundary. Every table also
contains a non-null `realm_id` and uses `(realm_id, id)` as its composite key,
so identically numbered QuickBooks objects cannot collide even if tables are
intentionally consolidated later.

Never reuse a secret scope, connection, pipeline checkpoint, or Job across
realms. Give each resource a stable tenant-qualified name rather than a
customer display name that may change.

## Onboarding

1. Choose an immutable internal tenant key and record the expected QuickBooks
   `realm_id`.
2. Create a dedicated secret scope. Grant secret-read access only to the
   tenant's refresh Job run identity and secret-manage access only to the
   onboarding operators.
3. Store the four required secrets. Do not copy an existing tenant's scope.
4. Create a dedicated COMMUNITY connection whose `realm_id` matches the scope.
   Set its comment to `quickbooks-realm-sha256:<sha256(realm_id)>`. This
   non-reversible marker is required because Databricks redacts connection
   options when the refresh task reads the connection.
5. Create a dedicated destination schema. Grant `USE CATALOG`, `USE SCHEMA`,
   and table write privileges only to the tenant's pipeline identity. Grant
   consumers read access separately.
6. Create a dedicated pipeline from
   `pipeline_spec.multi_tenant.yaml`, changing its connection and destination
   schema to the tenant-specific resources.
7. Create a dedicated refresh-then-ingest Job. Pass `expected_realm_id` as a
   non-secret parameter in addition to `secret_scope`, `connection_name`,
   `environment`, and `minor_version`.
8. Run the bootstrap through the Job, never by scheduling the pipeline
   directly. Validate that every row has the expected `realm_id`, that
   `(realm_id, id)` is unique, and that all offsets are version 2.

The refresh task refuses to rotate a token unless `expected_realm_id`, the
secret scope's `realm_id`, and the connection's realm-binding comment all
match.

## Runtime and failure isolation

Each tenant Job owns one refresh chain and one pipeline. Do not place multiple
tenants in one multi-task Job with cross-tenant dependencies. A token
revocation, API throttle, malformed record, or checkpoint failure then fails
only that tenant's run.

Version-2 offsets are bound to `realm_id`, table name, and flow (`updates` or
`deletes`). Passing state from another tenant, table, or flow fails before an
API request. Update and delete checkpoints remain independently committed by
Lakeflow.

## Revocation

1. Pause the tenant's Job schedule.
2. Revoke the Intuit authorization.
3. Delete the tenant's refresh token and OAuth client secrets from its scope.
4. Disable access to or delete the tenant's COMMUNITY connection according to
   workspace policy.
5. Revoke the pipeline identity's schema privileges.
6. Apply the agreed data-retention action below.

Do not delete shared OAuth application credentials if other explicitly
authorized tenants use the same Intuit application; remove only the revoked
tenant's authorization and scope.

## Data retention

Choose and record one policy during onboarding:

- **retain:** stop ingestion and keep tables read-only for the contractual
  retention period;
- **quarantine:** move access to a restricted schema while the retention clock
  runs;
- **purge:** drop only the tenant-specific schema after approval and the
  retention period.

If data was intentionally consolidated into shared tables, purge with an
explicit `realm_id` predicate and validate affected counts before committing.
Never use QuickBooks `id` alone for tenant deletion.

## Version-1 migration

M4 and earlier checkpoints do not contain tenant identity. Do not update an
existing pipeline in place. Create a new schema and pipeline, bootstrap with
version-2 offsets, validate parity, redirect consumers, and retire the old
pipeline under the selected retention policy.
