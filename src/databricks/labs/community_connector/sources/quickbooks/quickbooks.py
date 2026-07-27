"""Lakeflow community connector for QuickBooks Online.

Customers support a checkpointed snapshot-to-incremental handoff based on
``MetaData.LastUpdatedTime``. The other entities remain snapshot-only.
Deletion synchronization is tracked separately in ARCHITECTURE.md.
"""

from __future__ import annotations

import json
import random
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
from typing import Iterator

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.quickbooks.quickbooks_schemas import (
    TABLE_SCHEMAS,
)

TABLE_TO_ENTITY = {
    "customers": "Customer",
    "vendors": "Vendor",
    "accounts": "Account",
    "items": "Item",
    "invoices": "Invoice",
    "bills": "Bill",
}

RETRIABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
DEFAULT_PAGE_SIZE = 1000
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_MAX_RETRIES = 5
DEFAULT_INCREMENTAL_OVERLAP_SECONDS = 60
DEFAULT_MAX_INCREMENTAL_WINDOW_SECONDS = 86400
OFFSET_VERSION = 1
OFFSET_VERSION_KEY = "version"
OFFSET_CURSOR_KEY = "updated_through"
CUSTOMER_CURSOR_FIELD = "last_updated_at"


class QuickBooksApiClient:
    """Small, stateless QuickBooks Online query client."""

    def __init__(
        self,
        *,
        access_token: str,
        realm_id: str,
        environment: str,
        minor_version: int,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> None:
        if environment not in {"sandbox", "production"}:
            raise ValueError("environment must be 'sandbox' or 'production'")
        self._access_token = access_token
        self._realm_id = realm_id
        self._environment = environment
        self._minor_version = minor_version
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries

    @property
    def _base_url(self) -> str:
        if self._environment == "sandbox":
            return "https://sandbox-quickbooks.api.intuit.com"
        return "https://quickbooks.api.intuit.com"

    def iter_entity(
        self,
        entity: str,
        *,
        page_size: int,
        where_clause: str | None = None,
    ) -> Iterator[dict]:
        """Yield a complete positional QuickBooks query one page at a time."""
        start_position = 1
        while True:
            query = f"SELECT * FROM {entity}"
            if where_clause:
                query += f" WHERE {where_clause}"
            query += f" STARTPOSITION {start_position} MAXRESULTS {page_size}"
            body = self._get_query(query)
            page = body.get("QueryResponse", {}).get(entity, [])
            if not isinstance(page, list):
                raise RuntimeError(f"QuickBooks {entity} query returned an invalid row collection")
            yield from (row for row in page if isinstance(row, dict))
            if len(page) < page_size:
                return
            start_position += page_size

    def _get_query(self, query: str) -> dict:
        url = f"{self._base_url}/v3/company/{self._realm_id}/query"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._access_token}",
        }
        params = {"query": query, "minorversion": str(self._minor_version)}

        for attempt in range(self._max_retries):
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=self._timeout_seconds,
                )
            except requests.RequestException as exc:
                if attempt == self._max_retries - 1:
                    raise RuntimeError("QuickBooks request failed after retry exhaustion") from exc
                self._sleep_before_retry(attempt, None)
                continue

            if response.status_code in {401, 403}:
                raise PermissionError(
                    "QuickBooks authentication failed; refresh or recreate the "
                    "Unity Catalog connection"
                )
            if response.status_code not in RETRIABLE_STATUS_CODES:
                try:
                    response.raise_for_status()
                except requests.HTTPError as exc:
                    raise RuntimeError(
                        f"QuickBooks query failed with HTTP {response.status_code}"
                    ) from exc
                try:
                    payload = response.json()
                except ValueError as exc:
                    raise RuntimeError("QuickBooks returned an invalid JSON response") from exc
                if not isinstance(payload, dict):
                    raise RuntimeError("QuickBooks returned an invalid JSON object")
                return payload

            if attempt == self._max_retries - 1:
                raise RuntimeError(
                    "QuickBooks request failed after retry exhaustion "
                    f"(HTTP {response.status_code})"
                )
            self._sleep_before_retry(attempt, response.headers.get("Retry-After"))

        raise AssertionError("unreachable retry state")

    @staticmethod
    def _sleep_before_retry(attempt: int, retry_after: str | None) -> None:
        delay = _retry_after_seconds(retry_after)
        if delay is None:
            delay = min(2**attempt, 30) + random.uniform(0, 0.25)
        time.sleep(delay)


class QuickBooksLakeflowConnect(LakeflowConnect):
    """QuickBooks connector with Customer incremental-update support."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        access_token = options.get("access_token", "").strip()
        realm_id = options.get("realm_id", "").strip()
        if not access_token:
            raise ValueError(
                "QuickBooks requires an access_token injected by the Unity Catalog OAuth connection"
            )
        if not realm_id:
            raise ValueError("QuickBooks requires realm_id for the authorized company")

        self._client = QuickBooksApiClient(
            access_token=access_token,
            realm_id=realm_id,
            environment=options.get("environment", "production").strip().lower(),
            minor_version=int(options.get("minor_version", "75")),
            timeout_seconds=int(options.get("timeout_seconds", str(DEFAULT_TIMEOUT_SECONDS))),
            max_retries=int(options.get("max_retries", str(DEFAULT_MAX_RETRIES))),
        )
        # Freeze the upper bound for this Data Source instance. AvailableNow
        # must converge instead of chasing records written while it is running.
        self._init_ts = _format_qbo_datetime(_utc_now())

    def list_tables(self) -> list[str]:
        return list(TABLE_TO_ENTITY)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        del table_options
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        del table_options
        self._validate_table(table_name)
        if table_name == "customers":
            return {
                "primary_keys": ["id"],
                "cursor_field": CUSTOMER_CURSOR_FIELD,
                "ingestion_type": "cdc",
            }
        return {
            "primary_keys": ["id"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        page_size = int(table_options.get("page_size", str(DEFAULT_PAGE_SIZE)))
        if not 1 <= page_size <= 1000:
            raise ValueError("page_size must be between 1 and 1000")

        if table_name == "customers":
            return self._read_customers_incrementally(
                start_offset,
                table_options,
                page_size=page_size,
            )

        entity = TABLE_TO_ENTITY[table_name]
        records = (
            _normalize_entity(table_name, row)
            for row in self._client.iter_entity(entity, page_size=page_size)
        )
        return records, {}

    def _read_customers_incrementally(
        self,
        start_offset: dict,
        table_options: dict[str, str],
        *,
        page_size: int,
    ) -> tuple[Iterator[dict], dict]:
        cursor = _parse_customer_offset(start_offset)
        init_dt = _parse_qbo_datetime(self._init_ts)

        # First call: emit the complete snapshot, but checkpoint the time at
        # which this reader was initialized. Changes racing with the snapshot
        # are replayed by the overlap on the next trigger.
        if cursor is None:
            records = (
                _normalize_customer_cdc(row)
                for row in self._client.iter_entity("Customer", page_size=page_size)
            )
            return records, _customer_offset(self._init_ts)

        cursor_dt = _parse_qbo_datetime(cursor)
        if cursor_dt >= init_dt:
            return iter([]), start_offset

        overlap_seconds = _bounded_int_option(
            table_options,
            "incremental_overlap_seconds",
            default=DEFAULT_INCREMENTAL_OVERLAP_SECONDS,
            minimum=0,
            maximum=3600,
        )
        max_window_seconds = _bounded_int_option(
            table_options,
            "max_incremental_window_seconds",
            default=DEFAULT_MAX_INCREMENTAL_WINDOW_SECONDS,
            minimum=60,
            maximum=604800,
        )
        lower_dt = cursor_dt - timedelta(seconds=overlap_seconds)
        upper_dt = min(
            cursor_dt + timedelta(seconds=max_window_seconds),
            init_dt,
        )
        lower = _format_qbo_datetime(lower_dt)
        upper = _format_qbo_datetime(upper_dt)
        where_clause = (
            f"MetaData.LastUpdatedTime >= '{lower}' AND MetaData.LastUpdatedTime <= '{upper}'"
        )
        records = (
            _normalize_customer_cdc(row)
            for row in self._client.iter_entity(
                "Customer",
                page_size=page_size,
                where_clause=where_clause,
            )
        )
        return records, _customer_offset(upper)

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLE_TO_ENTITY:
            raise ValueError(
                f"Unsupported QuickBooks table '{table_name}'. "
                f"Supported tables: {self.list_tables()}"
            )


def _normalize_entity(table_name: str, row: dict) -> dict:
    entity_id = row.get("Id")
    if entity_id in {None, ""}:
        raise RuntimeError("QuickBooks entity is missing Id")
    metadata = row.get("MetaData") if isinstance(row.get("MetaData"), dict) else {}
    common = {
        "id": str(entity_id),
        "sync_token": _optional_string(row.get("SyncToken")),
        "created_at": _optional_datetime(metadata.get("CreateTime")),
        "last_updated_at": _optional_datetime(metadata.get("LastUpdatedTime")),
        "raw_json": json.dumps(row, separators=(",", ":"), sort_keys=True),
    }
    normalizers = {
        "customers": _normalize_customer,
        "vendors": _normalize_vendor,
        "accounts": _normalize_account,
        "items": _normalize_item,
        "invoices": _normalize_invoice,
        "bills": _normalize_bill,
    }
    return common | normalizers[table_name](row)


def _normalize_customer_cdc(row: dict) -> dict:
    record = _normalize_entity("customers", row)
    if record[CUSTOMER_CURSOR_FIELD] is None:
        raise RuntimeError("QuickBooks Customer is missing MetaData.LastUpdatedTime")
    return record


def _normalize_customer(row: dict) -> dict:
    return _party_fields(row) | {
        "balance": _optional_decimal(row.get("Balance")),
        "currency_ref": _reference_value(row.get("CurrencyRef")),
        "active": _optional_bool(row.get("Active")),
    }


def _normalize_vendor(row: dict) -> dict:
    return _party_fields(row) | {
        "balance": _optional_decimal(row.get("Balance")),
        "vendor_1099": _optional_bool(row.get("Vendor1099")),
        "currency_ref": _reference_value(row.get("CurrencyRef")),
        "active": _optional_bool(row.get("Active")),
    }


def _party_fields(row: dict) -> dict:
    return {
        "display_name": _optional_string(row.get("DisplayName")),
        "company_name": _optional_string(row.get("CompanyName")),
        "given_name": _optional_string(row.get("GivenName")),
        "family_name": _optional_string(row.get("FamilyName")),
        "primary_email": _nested_string(row, "PrimaryEmailAddr", "Address"),
        "primary_phone": _nested_string(row, "PrimaryPhone", "FreeFormNumber"),
    }


def _normalize_account(row: dict) -> dict:
    return {
        "name": _optional_string(row.get("Name")),
        "fully_qualified_name": _optional_string(row.get("FullyQualifiedName")),
        "account_type": _optional_string(row.get("AccountType")),
        "account_sub_type": _optional_string(row.get("AccountSubType")),
        "classification": _optional_string(row.get("Classification")),
        "current_balance": _optional_decimal(row.get("CurrentBalance")),
        "currency_ref": _reference_value(row.get("CurrencyRef")),
        "active": _optional_bool(row.get("Active")),
    }


def _normalize_item(row: dict) -> dict:
    return {
        "name": _optional_string(row.get("Name")),
        "fully_qualified_name": _optional_string(row.get("FullyQualifiedName")),
        "item_type": _optional_string(row.get("Type")),
        "description": _optional_string(row.get("Description")),
        "unit_price": _optional_decimal(row.get("UnitPrice")),
        "purchase_cost": _optional_decimal(row.get("PurchaseCost")),
        "quantity_on_hand": _optional_decimal(row.get("QtyOnHand")),
        "income_account_ref": _reference_value(row.get("IncomeAccountRef")),
        "expense_account_ref": _reference_value(row.get("ExpenseAccountRef")),
        "asset_account_ref": _reference_value(row.get("AssetAccountRef")),
        "active": _optional_bool(row.get("Active")),
    }


def _normalize_invoice(row: dict) -> dict:
    return _transaction_fields(row) | {
        "customer_ref": _reference_value(row.get("CustomerRef")),
        "email_status": _optional_string(row.get("EmailStatus")),
        "print_status": _optional_string(row.get("PrintStatus")),
    }


def _normalize_bill(row: dict) -> dict:
    return _transaction_fields(row) | {
        "vendor_ref": _reference_value(row.get("VendorRef")),
        "ap_account_ref": _reference_value(row.get("APAccountRef")),
    }


def _transaction_fields(row: dict) -> dict:
    lines = row.get("Line")
    return {
        "doc_number": _optional_string(row.get("DocNumber")),
        "txn_date": _optional_date(row.get("TxnDate")),
        "due_date": _optional_date(row.get("DueDate")),
        "total_amount": _optional_decimal(row.get("TotalAmt")),
        "balance": _optional_decimal(row.get("Balance")),
        "currency_ref": _reference_value(row.get("CurrencyRef")),
        "line_json": (
            json.dumps(lines, separators=(",", ":"), sort_keys=True)
            if isinstance(lines, list)
            else None
        ),
    }


def _optional_string(value: object) -> str | None:
    if value in {None, ""}:
        return None
    return str(value)


def _optional_decimal(value: object) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except (ArithmeticError, ValueError) as exc:
        raise RuntimeError(f"Invalid QuickBooks decimal value: {value!r}") from exc


def _optional_datetime(value: object) -> datetime | None:
    if value in {None, ""}:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise RuntimeError(f"Invalid QuickBooks timestamp: {value!r}") from exc


def _optional_date(value: object) -> str | None:
    if value in {None, ""}:
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise RuntimeError(f"Invalid QuickBooks date: {value!r}") from exc


def _optional_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _nested_string(row: dict, parent: str, child: str) -> str | None:
    value = row.get(parent)
    if not isinstance(value, dict):
        return None
    return _optional_string(value.get(child))


def _reference_value(value: object) -> str | None:
    if not isinstance(value, dict):
        return None
    return _optional_string(value.get("value"))


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            target = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        now = datetime.now(target.tzinfo)
        return max(0.0, (target - now).total_seconds())


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_qbo_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("QuickBooks cursor datetime must include a timezone")
    utc_value = value.astimezone(timezone.utc)
    return utc_value.isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_qbo_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise ValueError(f"Invalid QuickBooks cursor timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"QuickBooks cursor timestamp must include a timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


def _customer_offset(updated_through: str) -> dict:
    return {
        OFFSET_VERSION_KEY: OFFSET_VERSION,
        OFFSET_CURSOR_KEY: updated_through,
    }


def _parse_customer_offset(start_offset: dict) -> str | None:
    if not start_offset:
        return None
    if start_offset.get(OFFSET_VERSION_KEY) != OFFSET_VERSION:
        raise ValueError(
            f"Unsupported QuickBooks Customer offset version: "
            f"{start_offset.get(OFFSET_VERSION_KEY)!r}"
        )
    cursor = start_offset.get(OFFSET_CURSOR_KEY)
    if not isinstance(cursor, str) or not cursor:
        raise ValueError(f"QuickBooks Customer offset requires non-empty '{OFFSET_CURSOR_KEY}'")
    _parse_qbo_datetime(cursor)
    return cursor


def _bounded_int_option(
    options: dict[str, str],
    name: str,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    try:
        value = int(options.get(name, str(default)))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value
