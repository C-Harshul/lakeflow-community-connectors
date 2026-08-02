"""Static Spark schemas and table metadata for the Ramp connector.

The field inventory is derived from Ramp's Developer API OpenAPI document and
the decisions recorded in ``ramp_api_doc.md``. Dynamic accounting fields and
large variable nested collections are represented as canonical JSON strings;
stable nested records remain typed structs or arrays.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

MONEY_DECIMAL = DecimalType(38, 9)

CURRENCY_AMOUNT_STRUCT = StructType(
    [
        StructField("amount", LongType(), True),
        StructField("currency_code", StringType(), True),
        StructField("minor_unit_conversion_rate", LongType(), True),
    ]
)

API_AMOUNT_STRUCT = StructType(
    [
        StructField("currency", StringType(), True),
        StructField("value", LongType(), True),
    ]
)

SIGNED_AMOUNT_STRUCT = StructType(
    [
        StructField("currency", StringType(), True),
        StructField("value", LongType(), True),
    ]
)

TRANSACTION_CARD_HOLDER_STRUCT = StructType(
    [
        StructField("department_id", StringType(), True),
        StructField("department_name", StringType(), True),
        StructField("employee_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("location_id", StringType(), True),
        StructField("location_name", StringType(), True),
        StructField("user_id", StringType(), True),
    ]
)

TRANSACTION_DECLINE_DETAILS_STRUCT = StructType(
    [
        StructField("amount", MONEY_DECIMAL, True),
        StructField("declined_amount", SIGNED_AMOUNT_STRUCT, True),
        StructField("reason", StringType(), True),
    ]
)

MERCHANT_LOCATION_STRUCT = StructType(
    [
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("state", StringType(), True),
    ]
)

REIMBURSEMENT_TRACE_ID_STRUCT = StructType(
    [
        StructField("descriptor", StringType(), True),
        StructField("trace_id", StringType(), True),
    ]
)

ADDRESS_STRUCT = StructType(
    [
        StructField("address_line_1", StringType(), True),
        StructField("address_line_2", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("state", StringType(), True),
    ]
)

VENDOR_ADDRESS_STRUCT = StructType(
    [
        StructField("address_line_1", StringType(), True),
        StructField("address_line_2", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("id", StringType(), True),
        StructField("is_default", BooleanType(), True),
        StructField("postal_code", StringType(), True),
        StructField("state", StringType(), True),
    ]
)

DEFAULT_PAYMENT_METHOD_STRUCT = StructType(
    [
        # The policy is a discriminated union (address/bank-account/card), so
        # it remains canonical JSON while the stable update source is typed.
        StructField("policy", StringType(), True),
        StructField("update_source", StringType(), True),
    ]
)


TRANSACTIONS_SCHEMA = StructType(
    [
        StructField("accounting_categories", StringType(), True),
        StructField("accounting_date", TimestampType(), True),
        StructField("accounting_field_selections", StringType(), True),
        StructField("all_requirements_met_and_approved", BooleanType(), True),
        StructField("amount", MONEY_DECIMAL, True),
        StructField("attendees", StringType(), True),
        StructField("card_holder", TRANSACTION_CARD_HOLDER_STRUCT, True),
        StructField("card_id", StringType(), True),
        StructField("card_present", BooleanType(), True),
        StructField("currency_code", StringType(), True),
        StructField("decline_details", TRANSACTION_DECLINE_DETAILS_STRUCT, True),
        StructField("disputes", StringType(), True),
        StructField("entity_id", StringType(), True),
        StructField("fund_id", StringType(), True),
        StructField("id", StringType(), False),
        StructField("limit_id", StringType(), True),
        StructField("line_items", StringType(), True),
        StructField("memo", StringType(), True),
        StructField("merchant_category_code", StringType(), True),
        StructField("merchant_category_code_description", StringType(), True),
        StructField("merchant_data", StringType(), True),
        StructField("merchant_descriptor", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_location", MERCHANT_LOCATION_STRUCT, True),
        StructField("merchant_name", StringType(), True),
        StructField("minor_unit_conversion_rate", LongType(), True),
        StructField("network_merchant_id", StringType(), True),
        StructField("original_transaction_amount", CURRENCY_AMOUNT_STRUCT, True),
        StructField("original_transaction_id", StringType(), True),
        StructField("policy_violations", StringType(), True),
        StructField("receipts", ArrayType(StringType(), True), True),
        StructField(
            "requires_accounting_vendor_creation_to_sync",
            BooleanType(),
            True,
        ),
        StructField("settlement_date", TimestampType(), True),
        StructField("sk_category_id", LongType(), True),
        StructField("sk_category_name", StringType(), True),
        StructField("spend_program_id", StringType(), True),
        StructField("state", StringType(), True),
        StructField("statement_id", StringType(), True),
        StructField("sync_status", StringType(), True),
        StructField("synced_at", TimestampType(), True),
        StructField("trip_id", StringType(), True),
        StructField("trip_name", StringType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("user_transaction_time", TimestampType(), True),
        StructField("tenant_id", StringType(), False),
        StructField("_ramp_extracted_at", TimestampType(), False),
    ]
)

REIMBURSEMENTS_SCHEMA = StructType(
    [
        StructField("accounting_date", TimestampType(), True),
        StructField("accounting_field_selections", StringType(), True),
        StructField("approved_at", TimestampType(), True),
        StructField("attendees", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("direction", StringType(), True),
        StructField("distance", MONEY_DECIMAL, True),
        StructField("employee_id", StringType(), True),
        StructField("end_location", StringType(), True),
        StructField("entity_amount", API_AMOUNT_STRUCT, True),
        StructField("entity_id", StringType(), True),
        StructField("fund_id", StringType(), True),
        StructField("id", StringType(), False),
        StructField("line_items", StringType(), True),
        StructField("memo", StringType(), True),
        StructField("merchant", StringType(), True),
        StructField("merchant_amount", API_AMOUNT_STRUCT, True),
        StructField("merchant_id", StringType(), True),
        StructField("payee_amount", API_AMOUNT_STRUCT, True),
        StructField("payment_batch_id", StringType(), True),
        StructField("payment_id", StringType(), True),
        StructField("payment_processed_at", TimestampType(), True),
        StructField("receipts", ArrayType(StringType(), True), True),
        StructField("spend_limit_id", StringType(), True),
        StructField("start_location", StringType(), True),
        StructField("state", StringType(), True),
        StructField("submitted_at", TimestampType(), True),
        StructField("sync_status", StringType(), True),
        StructField("synced_at", TimestampType(), True),
        StructField("trace_id", REIMBURSEMENT_TRACE_ID_STRUCT, True),
        StructField("transaction_date", DateType(), True),
        StructField("trip_id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("user_email", StringType(), True),
        StructField("user_full_name", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("waypoints", ArrayType(StringType(), True), True),
        StructField("tenant_id", StringType(), False),
        StructField("_ramp_extracted_at", TimestampType(), False),
    ]
)

VENDORS_SCHEMA = StructType(
    [
        StructField("accounting_vendor_remote_id", StringType(), True),
        StructField("address", ADDRESS_STRUCT, True),
        StructField("addresses", ArrayType(VENDOR_ADDRESS_STRUCT, True), True),
        StructField("billing_frequency", StringType(), True),
        StructField("contacts", ArrayType(StringType(), True), True),
        StructField("country", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("default_entity_id", StringType(), True),
        StructField("default_payment_method", DEFAULT_PAYMENT_METHOD_STRUCT, True),
        StructField("description", StringType(), True),
        StructField("external_vendor_id", StringType(), True),
        StructField("federal_tax_classification", StringType(), True),
        StructField("id", StringType(), False),
        StructField("is_active", BooleanType(), True),
        StructField("is_deletable", BooleanType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("name_legal", StringType(), True),
        StructField("parent_vendor_id", StringType(), True),
        StructField("sk_category_id", LongType(), True),
        StructField("sk_category_name", StringType(), True),
        StructField("state", StringType(), True),
        StructField("subsidiary", ArrayType(StringType(), True), True),
        StructField("tax_address", ADDRESS_STRUCT, True),
        StructField("total_spend_all_time", CURRENCY_AMOUNT_STRUCT, True),
        StructField("total_spend_last_30_days", CURRENCY_AMOUNT_STRUCT, True),
        StructField("total_spend_last_365_days", CURRENCY_AMOUNT_STRUCT, True),
        StructField("total_spend_ytd", CURRENCY_AMOUNT_STRUCT, True),
        StructField("vendor_owner_id", StringType(), True),
        StructField("vendor_type", StringType(), True),
        StructField("tenant_id", StringType(), False),
        # Ramp filters vendors by updated_at but omits updated_at from rows.
        # M3/M4 will stamp the fully-drained update-window boundary here.
        StructField("_ramp_window_end", TimestampType(), True),
        StructField("_ramp_extracted_at", TimestampType(), False),
    ]
)

USERS_SCHEMA = StructType(
    [
        StructField("business_id", StringType(), True),
        StructField("custom_fields", StringType(), True),
        StructField("department_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("employee_id", StringType(), True),
        StructField("entity_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("id", StringType(), False),
        StructField("is_manager", BooleanType(), True),
        StructField("last_name", StringType(), True),
        StructField("location_id", StringType(), True),
        StructField("manager_id", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("role", StringType(), True),
        StructField("scheduled_deactivation_date", TimestampType(), True),
        StructField("scheduled_invitation_date", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("tenant_id", StringType(), False),
        StructField("_ramp_extracted_at", TimestampType(), False),
    ]
)

DEPARTMENTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("tenant_id", StringType(), False),
        StructField("_ramp_extracted_at", TimestampType(), False),
    ]
)

LOCATIONS_SCHEMA = StructType(
    [
        StructField("entity_id", StringType(), True),
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("tenant_id", StringType(), False),
        StructField("_ramp_extracted_at", TimestampType(), False),
    ]
)


TABLE_SCHEMAS: dict[str, StructType] = {
    "transactions": TRANSACTIONS_SCHEMA,
    "reimbursements": REIMBURSEMENTS_SCHEMA,
    "vendors": VENDORS_SCHEMA,
    "users": USERS_SCHEMA,
    "departments": DEPARTMENTS_SCHEMA,
    "locations": LOCATIONS_SCHEMA,
}

TABLE_METADATA: dict[str, dict] = {
    "transactions": {
        "primary_keys": ["tenant_id", "id"],
        "ingestion_type": "snapshot",
    },
    "reimbursements": {
        "primary_keys": ["tenant_id", "id"],
        "ingestion_type": "snapshot",
    },
    "vendors": {
        "primary_keys": ["tenant_id", "id"],
        "cursor_field": "_ramp_window_end",
        "ingestion_type": "cdc",
    },
    "users": {
        "primary_keys": ["tenant_id", "id"],
        "ingestion_type": "snapshot",
    },
    "departments": {
        "primary_keys": ["tenant_id", "id"],
        "ingestion_type": "snapshot",
    },
    "locations": {
        "primary_keys": ["tenant_id", "id"],
        "ingestion_type": "snapshot",
    },
}

SUPPORTED_TABLES: tuple[str, ...] = tuple(TABLE_SCHEMAS)

# Fields M3 must serialize deterministically before emission.
JSON_STRING_FIELDS: dict[str, frozenset[str]] = {
    "transactions": frozenset(
        {
            "accounting_categories",
            "accounting_field_selections",
            "attendees",
            "disputes",
            "line_items",
            "merchant_data",
            "policy_violations",
        }
    ),
    "reimbursements": frozenset(
        {
            "accounting_field_selections",
            "attendees",
            "line_items",
        }
    ),
    "vendors": frozenset(),
    "users": frozenset({"custom_fields"}),
    "departments": frozenset(),
    "locations": frozenset(),
}
