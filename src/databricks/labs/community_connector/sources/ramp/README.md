# Lakeflow Ramp Community Connector

This connector ingests Ramp spend and reference data into Databricks through
Lakeflow managed ingestion. It supports one Ramp business per Unity Catalog
connection and qualifies every source key with the authenticated Ramp business
ID.

## Prerequisites

- A Ramp account with permission to create and manage a developer application.
- A Ramp developer application belonging to the business you want to ingest.
- A Databricks workspace with Unity Catalog and Lakeflow managed ingestion.
- Permission to create or use Unity Catalog connections, volumes, pipelines,
  schemas, and destination tables.
- Network access from Databricks to the selected Ramp API environment.

## Setup

### Required Connection Parameters

The connection uses the Ramp application's OAuth identity. Unity Catalog
obtains access tokens automatically; users do not copy an access token into the
connector.

| Name | Type | Required | Description | Example |
|---|---|---|---|---|
| `client_id` | string | Yes | Client ID of the Ramp developer application. | `ramp_id_...` |
| `client_secret` | secret string | Yes | Client secret of the Ramp developer application. Store it only as a secret connection property. | Not shown |
| `environment` | string | No | Ramp API environment: `production` or `sandbox`. Defaults to `production`. The connection's OAuth token endpoint must match this value. | `sandbox` |
| `externalOptionsAllowList` | string | Yes | Definitive list of table-specific options allowed through the Unity Catalog connection. | `page_size,max_records_per_batch,window_seconds,lookback_seconds` |

The complete `externalOptionsAllowList` value is:

```text
page_size,max_records_per_batch,window_seconds,lookback_seconds
```

These names authorize table options; they are not Ramp credentials.

### Create the Ramp Application

1. Open the Ramp developer portal for the target business and create an
   internal developer application.
2. Grant these read scopes:

   ```text
   business:read
   transactions:read
   reimbursements:read
   vendors:read
   users:read
   departments:read
   locations:read
   ```

3. Copy the generated client ID and client secret into a secure secret manager.
4. Create separate applications for production and sandbox. Credentials and
   tokens are not portable between the two environments.

This connection authenticates machine-to-machine, so there is no browser
consent step and no redirect URI to register.

The standard connector specification targets Ramp production at
`https://api.ramp.com`. A sandbox connection must instead use
`https://demo-api.ramp.com/developer/v1/token` and pass
`environment=sandbox` to the connector.

### Create a Unity Catalog Connection

A Unity Catalog connection can be created from the Lakeflow Community
Connector flow on the Databricks **Add Data** page or with the community
connector CLI.

Configure the Ramp client ID and secret, select the matching environment, and
set `externalOptionsAllowList` to:

```text
page_size,max_records_per_batch,window_seconds,lookback_seconds
```

Never store credentials in pipeline specifications, notebooks, source files,
test fixtures, or logs.

## Supported Objects

Use the exact lowercase object names below. Every table uses the composite
primary key `[tenant_id, id]`, where `tenant_id` is the authenticated Ramp
business ID.

| Object | Description | Ingestion type | Incremental cursor |
|---|---|---|---|
| `transactions` | Card and spend transactions, including declined transactions through `state=ALL`. | Snapshot | Not applicable |
| `reimbursements` | Reimbursements in both business-to-user and user-to-business directions. | Snapshot | Not applicable |
| `vendors` | Active, inactive, and draft vendors. | CDC upsert | `_ramp_window_end` |
| `users` | Users across active, draft, inactive, and suspended statuses. | Snapshot | Not applicable |
| `departments` | Ramp departments. | Snapshot | Not applicable |
| `locations` | Ramp locations. | Snapshot | Not applicable |

Transactions and reimbursements intentionally use complete snapshots. Their
APIs provide lower update-time filters but no matching upper update boundary
or compatible cursor ordering, so advancing an incremental checkpoint could
skip concurrent changes.

Vendors support bounded update windows. The connector freezes an upper bound,
fully drains each `(from_updated_at, to_updated_at]` window, and checkpoints
the complete opaque Ramp pagination URL when a batch stops between pages. A
configurable lookback overlaps the first window of each trigger. Vendor rows
carry `_ramp_window_end`, the fully drained server window boundary.

No object advertises delete synchronization. Lifecycle values such as inactive
vendors or deleted reimbursement states remain ordinary upserted row fields.

Schema highlights:

- `_ramp_extracted_at` records UTC connector extraction time on every row.
- Dynamic transaction and reimbursement structures are stored as canonical
  JSON strings where a fixed Spark schema is not reliable.
- Vendor payment-policy variants are stored as canonical JSON inside the typed
  `default_payment_method` structure.
- Missing optional Ramp properties become null rather than changing the
  declared schema.

## Table Configurations

### Source & Destination

These values are set directly on each `table` object in a managed pipeline
specification:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | One of the six supported lowercase Ramp object names. |
| `destination_catalog` | No | Target catalog; defaults to the pipeline catalog. |
| `destination_schema` | No | Target schema; defaults to the pipeline schema. |
| `destination_table` | No | Target table; defaults to `source_table`. |

### Common `table_configuration` Options

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. |
| `primary_keys` | No | Overrides the default `[tenant_id, id]` key. Changing it is not recommended. |
| `sequence_by` | No | Column used to order SCD Type 2 changes. |
| `cluster_by` | No | Destination columns used for Liquid Clustering. |

### Source-Specific Options

In a managed-ingestion pipeline specification, place these values under
`connector_options.community_connector_options.options` on the table.

| Option | Objects | Required | Default | Validation and behavior |
|---|---|---|---|---|
| `page_size` | All | No | `100` | Integer from 2 through 100. |
| `max_records_per_batch` | `vendors` | No | `5000` | Integer of at least 2. Only complete Ramp pages are admitted, so a batch may intentionally contain fewer rows than the cap. |
| `window_seconds` | `vendors` | No | `86400` | Positive integer controlling the maximum update-window duration. |
| `lookback_seconds` | `vendors` | No | `300` | Non-negative integer subtracted once from the previously committed cursor at the beginning of a trigger. |

Business filters such as transaction state, reimbursement direction, user
status, or arbitrary start timestamps are not exposed because they would make
the corresponding table incomplete.

## Data Type Mapping

| Ramp value | Spark type | Notes |
|---|---|---|
| UUID, enum, or text | `StringType` | Preserved exactly. |
| ISO 8601 date-time | `TimestampType` | Parsed as an offset-aware UTC timestamp. |
| Calendar date | `DateType` | Stored without timezone conversion. |
| Boolean | `BooleanType` | Null remains distinct from false. |
| Integer or minor-unit amount | `LongType` | Used for integral counts and canonical money components. |
| Legacy decimal amount or distance | `DecimalType` | Parsed without binary floating-point conversion. |
| Stable nested object | `StructType` | Used for addresses, money objects, card-holder details, and similar records. |
| Stable repeated object | `ArrayType` | Element type is explicitly declared. |
| Dynamic object or array | `StringType` | Stored as deterministic compact JSON. |

## How to Run

### Step 1: Add the Connector

Use the Lakeflow Community Connector UI or the community connector CLI to
build and attach the Ramp connector package to a managed ingestion pipeline.

### Step 2: Configure the Pipeline

Create one table entry for every Ramp object to ingest. The following example
uses a production or sandbox connection that has already been configured with
the matching token endpoint:

```yaml
name: ramp_ingestion
catalog: main
schema: raw_ramp
serverless: true
channel: PREVIEW
ingestion_definition:
  connection_name: ramp_connection
  objects:
    - table:
        source_schema: default
        source_table: transactions
        destination_catalog: main
        destination_schema: raw_ramp
        connector_options:
          community_connector_options:
            options:
              page_size: "100"
    - table:
        source_schema: default
        source_table: vendors
        destination_catalog: main
        destination_schema: raw_ramp
        connector_options:
          community_connector_options:
            options:
              page_size: "100"
              max_records_per_batch: "5000"
              window_seconds: "86400"
              lookback_seconds: "300"
```

### Step 3: Run and Schedule the Pipeline

Run the pipeline once and inspect its event log and destination tables before
adding a schedule. Ramp's published default rate limit is 200 requests per 10
seconds per source IP. The connector retries rate limits and transient server
errors with bounded backoff.

#### Best Practices

- Start with the compact reference tables and vendors before running the two
  potentially large snapshot tables.
- Use a smaller vendor `window_seconds` when update volume is high.
- Keep the default lookback unless duplicate upserts have been evaluated; SCD
  Type 1 merges absorb the overlap by primary key.
- Use a separate Unity Catalog connection and destination schema for every
  Ramp business and environment.
- Rotate the Ramp client secret according to your organization's security
  policy and immediately after suspected disclosure.

#### Troubleshooting

- **401 authentication failure**: verify that the client ID, secret, token URL,
  and `environment` all belong to the same Ramp environment.
- **403 or missing-scope failure**: grant every required read scope to the Ramp
  application and recreate or update the Unity Catalog connection.
- **429 response**: reduce concurrent Ramp ingestion or allow the connector's
  bounded retry policy to back off.
- **Empty or incomplete users**: confirm that the application can read all
  documented user statuses.
- **Snapshot duration**: transactions and reimbursements intentionally scan all
  pages on each trigger; schedule them according to account size and API budget.
- **Invalid table option**: confirm the option applies to that object and is in
  `externalOptionsAllowList`.

## References

- [Ramp Developer API](https://docs.ramp.com/developer-api/)
- [Ramp authorization](https://docs.ramp.com/developer-api/v1/authorization)
- [Ramp sandbox](https://docs.ramp.com/developer-api/v1/sandbox)
- [Ramp pagination](https://docs.ramp.com/developer-api/v1/pagination)
- [Ramp rate limiting](https://docs.ramp.com/developer-api/v1/rate-limiting)
- [Databricks Lakeflow Community Connectors](https://github.com/databrickslabs/lakeflow-community-connectors)
