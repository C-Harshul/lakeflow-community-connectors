"""Read-only Ramp Developer API connector with lossless bounded vendor CDC."""

import json
import random
import re
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
from typing import Any, Iterator, Literal, TypedDict, cast
from urllib.parse import urlsplit

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.ramp.ramp_schemas import (
    JSON_STRING_FIELDS,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)

Environment = Literal["production", "sandbox"]

API_ROOTS: dict[Environment, str] = {
    "production": "https://api.ramp.com",
    "sandbox": "https://demo-api.ramp.com",
}

BUSINESS_ENDPOINT = "/developer/v1/business"
OFFSET_VERSION = 1
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 100
DEFAULT_MAX_RECORDS_PER_BATCH = 5000
DEFAULT_WINDOW_SECONDS = 86_400
DEFAULT_LOOKBACK_SECONDS = 300
HTTP_TIMEOUT_SECONDS = (10.0, 30.0)
MAX_HTTP_ATTEMPTS = 5
MAX_BACKOFF_SECONDS = 30.0
MAX_PAGES_PER_TRIGGER = 100_000
RETRIABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
_SAFE_CONTEXT = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


class RampApiError(RuntimeError):
    """Sanitized Ramp transport, HTTP, or response-contract failure."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        error_code: str | None = None,
        trace_id: str | None = None,
    ) -> None:
        details = [message]
        if status_code is not None:
            details.append(f"status={status_code}")
        if error_code:
            details.append(f"error_code={error_code}")
        if trace_id:
            details.append(f"trace_id={trace_id}")
        super().__init__("; ".join(details))
        self.status_code = status_code
        self.error_code = error_code
        self.trace_id = trace_id


class RampOffset(TypedDict, total=False):
    """Versioned, tenant-safe checkpoint envelope used by Ramp reads."""

    version: int
    table: str
    tenant_id: str
    environment: Environment
    flow: Literal["records"]
    phase: Literal["initial", "incremental", "complete"]
    cursor: str
    upper_bound: str
    continuation: str
    previous_continuation: str
    page_count: int
    window_start: str
    window_end: str
    lookback_applied: bool
    fanout_index: int


@dataclass(frozen=True)
class RampTableContract:
    """Immutable request and cursor contract for one supported table."""

    endpoint: str
    scope: str
    default_params: tuple[tuple[str, str], ...] = ()
    fanout: tuple[tuple[str, str], ...] = ()
    cursor_filter: str | None = None
    upper_cursor_filter: str | None = None
    lower_bound_exclusive: bool | None = None
    upper_bound_inclusive: bool | None = None


TABLE_CONTRACTS: dict[str, RampTableContract] = {
    "transactions": RampTableContract(
        endpoint="/developer/v1/transactions",
        scope="transactions:read",
        default_params=(("state", "ALL"),),
    ),
    "reimbursements": RampTableContract(
        endpoint="/developer/v1/reimbursements",
        scope="reimbursements:read",
        fanout=(
            ("direction", "BUSINESS_TO_USER"),
            ("direction", "USER_TO_BUSINESS"),
        ),
    ),
    "vendors": RampTableContract(
        endpoint="/developer/v1/vendors",
        scope="vendors:read",
        default_params=(("include_draft", "true"),),
        cursor_filter="from_updated_at",
        upper_cursor_filter="to_updated_at",
        lower_bound_exclusive=True,
        upper_bound_inclusive=True,
    ),
    "users": RampTableContract(
        endpoint="/developer/v1/users",
        scope="users:read",
        fanout=(
            ("status", "USER_ACTIVE"),
            ("status", "USER_DRAFT"),
            ("status", "USER_INACTIVE"),
            ("status", "USER_SUSPENDED"),
        ),
    ),
    "departments": RampTableContract(
        endpoint="/developer/v1/departments",
        scope="departments:read",
    ),
    "locations": RampTableContract(
        endpoint="/developer/v1/locations",
        scope="locations:read",
    ),
}

TABLE_ENDPOINTS: dict[str, str] = {
    table_name: contract.endpoint for table_name, contract in TABLE_CONTRACTS.items()
}

TABLE_SCOPES: dict[str, str] = {
    table_name: contract.scope for table_name, contract in TABLE_CONTRACTS.items()
}

DOWNGRADED_SNAPSHOT_TABLES = frozenset({"transactions", "reimbursements"})

TABLE_OPTION_ALLOWLIST: dict[str, frozenset[str]] = {
    "transactions": frozenset({"page_size"}),
    "reimbursements": frozenset({"page_size"}),
    "vendors": frozenset(
        {
            "lookback_seconds",
            "max_records_per_batch",
            "page_size",
            "window_seconds",
        }
    ),
    "users": frozenset({"page_size"}),
    "departments": frozenset({"page_size"}),
    "locations": frozenset({"page_size"}),
}

# Managed ingestion passes connection credentials and Databricks execution
# metadata through the same Spark options mapping as table controls. Unity
# Catalog has already restricted user-supplied table controls with the
# connector's external_options_allowlist, so the presence of one of these
# reserved markers means unrelated runtime keys must be ignored here.
MANAGED_RUNTIME_OPTION_MARKERS = frozenset(
    {
        "connectionname",
        "databricks.connection",
        "dltpipelineid",
    }
)

DEFAULT_TABLE_OPTIONS: dict[str, dict[str, str]] = {
    "transactions": {"page_size": str(DEFAULT_PAGE_SIZE)},
    "reimbursements": {"page_size": str(DEFAULT_PAGE_SIZE)},
    "vendors": {
        "page_size": str(DEFAULT_PAGE_SIZE),
        "max_records_per_batch": str(DEFAULT_MAX_RECORDS_PER_BATCH),
        "window_seconds": str(DEFAULT_WINDOW_SECONDS),
        "lookback_seconds": str(DEFAULT_LOOKBACK_SECONDS),
    },
    "users": {"page_size": str(DEFAULT_PAGE_SIZE)},
    "departments": {"page_size": str(DEFAULT_PAGE_SIZE)},
    "locations": {"page_size": str(DEFAULT_PAGE_SIZE)},
}


class RampApiClient:  # pylint: disable=too-many-instance-attributes
    """Hardened bearer-token client for Ramp's read-only Developer API."""

    def __init__(
        self,
        options: dict[str, str],
        *,
        session: Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        raw_environment = options.get("environment", "production").strip().lower()
        if raw_environment not in API_ROOTS:
            supported = ", ".join(sorted(API_ROOTS))
            raise ValueError(f"Unsupported Ramp environment. Expected one of: {supported}")

        access_token = options.get("access_token", "").strip()
        if not access_token:
            raise ValueError(
                "Ramp connector requires an OAuth access_token injected by "
                "the Unity Catalog M2M connection"
            )

        self.environment = cast(Environment, raw_environment)
        self.base_url = API_ROOTS[self.environment]
        self._access_token = access_token
        self.timeout_seconds = HTTP_TIMEOUT_SECONDS
        self._session = session or requests.Session()
        self._session.headers.update(self.authorization_headers())
        self._sleep = sleep
        self._jitter = jitter
        self._business_id: str | None = None

    def authorization_headers(self) -> dict[str, str]:
        """Build standard Ramp JSON request headers without logging them."""
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._access_token}",
        }

    def request_json(
        self,
        method: str,
        path_or_url: str,
        *,
        params: dict[str, str] | None = None,
    ) -> Any:
        """Issue a bounded request and return precision-preserving decoded JSON."""
        normalized_method = method.upper()
        if normalized_method != "GET":
            raise ValueError(f"Unsupported Ramp HTTP method: {normalized_method}")

        url = self._resolve_url(path_or_url)
        for attempt in range(MAX_HTTP_ATTEMPTS):
            try:
                response = self._session.request(
                    normalized_method,
                    url,
                    params=params,
                    timeout=self.timeout_seconds,
                    allow_redirects=False,
                )
            except (requests.ConnectionError, requests.Timeout):
                if attempt == MAX_HTTP_ATTEMPTS - 1:
                    raise RampApiError("Ramp transport failed after bounded retries") from None
                self._sleep(self._backoff_seconds(attempt, None))
                continue

            if 200 <= response.status_code < 300:
                try:
                    return response.json(parse_float=Decimal)
                except (TypeError, ValueError):
                    raise RampApiError(
                        "Ramp response was not valid JSON",
                        status_code=response.status_code,
                        trace_id=self._safe_context(response.headers.get("x-trace-id")),
                    ) from None

            if response.status_code in RETRIABLE_STATUS_CODES and attempt < MAX_HTTP_ATTEMPTS - 1:
                retry_after = response.headers.get("Retry-After")
                self._sleep(self._backoff_seconds(attempt, retry_after))
                continue

            raise self._http_error(response)

        raise RampApiError("Ramp request exhausted its retry policy")

    def iter_list(
        self,
        endpoint: str,
        *,
        params: dict[str, str] | None = None,
    ) -> Iterable[dict[str, Any]]:
        """Yield all list rows while validating each Ramp ``page.next`` URL."""
        target = endpoint
        request_params = dict(params) if params else None
        seen_continuations: set[str] = set()

        while True:
            data, next_url = self.get_list_page(target, params=request_params)
            request_params = None
            yield from data
            if next_url is None:
                return
            if next_url in seen_continuations:
                raise RampApiError("Ramp pagination returned a repeated continuation URL")
            seen_continuations.add(next_url)
            target = next_url

    def get_list_page(
        self,
        target: str,
        *,
        params: dict[str, str] | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Return one validated Ramp list page and its opaque continuation."""
        body = self.request_json("GET", target, params=params)
        if not isinstance(body, dict):
            raise RampApiError("Ramp list response must be a JSON object")

        data = body.get("data")
        if not isinstance(data, list):
            raise RampApiError("Ramp list response has invalid data")
        if any(not isinstance(record, dict) for record in data):
            raise RampApiError("Ramp list data contains a non-object record")

        page = body.get("page")
        if not isinstance(page, dict):
            raise RampApiError("Ramp list response has invalid page")
        next_url = page.get("next")
        if next_url is not None:
            if not isinstance(next_url, str) or not next_url:
                raise RampApiError("Ramp list response has invalid page.next")
            self._validate_continuation_url(next_url)
        return data, next_url

    def get_business_id(self) -> str:
        """Resolve and cache the immutable tenant ID for this authenticated app."""
        if self._business_id is not None:
            return self._business_id

        body = self.request_json("GET", BUSINESS_ENDPOINT)
        business_id = body.get("id") if isinstance(body, dict) else None
        if not isinstance(business_id, str) or not business_id.strip():
            raise RampApiError("Ramp business id was missing or invalid")
        self._business_id = business_id.strip()
        return self._business_id

    def _resolve_url(self, path_or_url: str) -> str:
        if path_or_url.startswith("https://") or path_or_url.startswith("http://"):
            self._validate_continuation_url(path_or_url)
            return path_or_url
        if not path_or_url.startswith("/developer/v1/"):
            raise ValueError("Ramp API path must start with /developer/v1/")
        return f"{self.base_url}{path_or_url}"

    def _validate_continuation_url(self, url: str) -> None:
        candidate = urlsplit(url)
        expected = urlsplit(self.base_url)
        if candidate.scheme != "https":
            raise RampApiError("Ramp continuation URL must use HTTPS")
        if candidate.netloc != expected.netloc:
            raise RampApiError("Ramp continuation URL has an unexpected host")
        if not candidate.path.startswith("/developer/v1/"):
            raise RampApiError("Ramp continuation URL has an unexpected path")
        if ".." in candidate.path.split("/"):
            raise RampApiError("Ramp continuation URL has an unsafe path")
        if candidate.fragment:
            raise RampApiError("Ramp continuation URL must not contain a fragment")

    def _backoff_seconds(self, attempt: int, retry_after: str | None) -> float:
        parsed_retry_after = self._parse_retry_after(retry_after)
        if parsed_retry_after is not None:
            return min(MAX_BACKOFF_SECONDS, parsed_retry_after)
        base = min(MAX_BACKOFF_SECONDS, float(2**attempt))
        return min(MAX_BACKOFF_SECONDS, base * (1.0 + 0.25 * self._jitter()))

    @staticmethod
    def _parse_retry_after(value: str | None) -> float | None:
        if not value:
            return None
        try:
            return max(0.0, float(value))
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(value)
            except (TypeError, ValueError, OverflowError):
                return None
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())

    @classmethod
    def _http_error(cls, response: requests.Response) -> RampApiError:
        error_code = None
        try:
            body = response.json()
        except (TypeError, ValueError):
            body = None
        if isinstance(body, dict):
            error_code = cls._safe_context(body.get("error_code"))
        return RampApiError(
            "Ramp API request failed",
            status_code=response.status_code,
            error_code=error_code,
            trace_id=cls._safe_context(response.headers.get("x-trace-id")),
        )

    @staticmethod
    def _safe_context(value: Any) -> str | None:
        if isinstance(value, str) and _SAFE_CONTEXT.fullmatch(value):
            return value
        return None


class RampLakeflowConnect(LakeflowConnect):
    """Lakeflow connector for complete snapshots and bounded vendor changes."""

    def __init__(
        self,
        options: dict[str, str],
        *,
        api_client: Any | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        super().__init__(options)
        self._api = api_client or RampApiClient(options)
        current_time = (now or (lambda: datetime.now(timezone.utc)))()
        if current_time.tzinfo is None:
            raise ValueError("Ramp connector clock must return a timezone-aware datetime")
        self.initial_snapshot_upper_bound = self._format_timestamp(current_time)

    def list_tables(self) -> list[str]:
        """Return the six M1-selected Ramp tables in stable order."""
        return list(SUPPORTED_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        self._validate_table_options(table_name, table_options)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        self._validate_table_options(table_name, table_options)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self,
        table_name: str,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict | None]:
        """Read a complete snapshot or one bounded, checkpointable vendor batch."""
        self._validate_table(table_name)
        self._validate_table_options(table_name, table_options)
        if start_offset is None:
            start_offset = {}
        elif not isinstance(start_offset, dict):
            raise ValueError("Ramp offset must be a dictionary")
        tenant_id = self._api.get_business_id()
        metadata = TABLE_METADATA[table_name]

        if metadata["ingestion_type"] == "snapshot":
            if start_offset:
                raise ValueError(f"Snapshot table {table_name!r} does not accept an offset")
            rows = self._read_complete_snapshot(table_name, tenant_id, table_options)
            return iter(rows), None

        if table_name != "vendors":
            raise AssertionError(f"Unexpected incremental Ramp table: {table_name}")
        rows, offset = self._read_vendors(tenant_id, start_offset, table_options)
        return iter(rows), offset

    def _read_vendors(
        self,
        tenant_id: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[list[dict[str, Any]], RampOffset]:
        """Read one initial page group or one closed vendor update window."""
        if not start_offset:
            upper_bound = self.initial_snapshot_upper_bound
            return self._read_vendor_pages(
                tenant_id=tenant_id,
                table_options=table_options,
                phase="initial",
                committed_cursor=None,
                upper_bound=upper_bound,
                window_start=None,
                window_end=upper_bound,
                continuation=None,
                previous_continuation=None,
                page_count=0,
            )

        offset = self._validate_offset("vendors", tenant_id, start_offset)
        phase = offset["phase"]
        if phase == "initial":
            return self._read_vendor_pages(
                tenant_id=tenant_id,
                table_options=table_options,
                phase="initial",
                committed_cursor=None,
                upper_bound=offset["upper_bound"],
                window_start=None,
                window_end=offset["upper_bound"],
                continuation=offset["continuation"],
                previous_continuation=offset.get("previous_continuation"),
                page_count=offset.get("page_count", 0),
            )

        cursor = self._parse_timestamp("cursor", offset["cursor"])
        if phase == "complete":
            trigger_upper = self._parse_timestamp("upper_bound", self.initial_snapshot_upper_bound)
            if cursor >= trigger_upper:
                return [], cast(RampOffset, dict(start_offset))
            lookback_seconds = self._option_int(
                table_options, "lookback_seconds", DEFAULT_LOOKBACK_SECONDS
            )
            window_start = cursor - timedelta(seconds=lookback_seconds)
            upper_bound = trigger_upper
        else:
            upper_bound = self._parse_timestamp("upper_bound", offset["upper_bound"])
            if "continuation" in offset:
                return self._read_vendor_pages(
                    tenant_id=tenant_id,
                    table_options=table_options,
                    phase="incremental",
                    committed_cursor=offset["cursor"],
                    upper_bound=offset["upper_bound"],
                    window_start=offset["window_start"],
                    window_end=offset["window_end"],
                    continuation=offset["continuation"],
                    previous_continuation=offset.get("previous_continuation"),
                    page_count=offset.get("page_count", 0),
                )
            window_start = cursor

        window_seconds = self._option_int(table_options, "window_seconds", DEFAULT_WINDOW_SECONDS)
        window_end = min(cursor + timedelta(seconds=window_seconds), upper_bound)
        return self._read_vendor_pages(
            tenant_id=tenant_id,
            table_options=table_options,
            phase="incremental",
            committed_cursor=self._format_timestamp(cursor),
            upper_bound=self._format_timestamp(upper_bound),
            window_start=self._format_timestamp(window_start),
            window_end=self._format_timestamp(window_end),
            continuation=None,
            previous_continuation=None,
            page_count=0,
        )

    def _read_vendor_pages(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        *,
        tenant_id: str,
        table_options: dict[str, str],
        phase: Literal["initial", "incremental"],
        committed_cursor: str | None,
        upper_bound: str,
        window_start: str | None,
        window_end: str,
        continuation: str | None,
        previous_continuation: str | None,
        page_count: int,
    ) -> tuple[list[dict[str, Any]], RampOffset]:
        """Drain only whole source pages and checkpoint the next opaque URL."""
        configured_page_size = self._option_int(table_options, "page_size", DEFAULT_PAGE_SIZE)
        max_records = self._option_int(
            table_options,
            "max_records_per_batch",
            DEFAULT_MAX_RECORDS_PER_BATCH,
        )
        effective_page_size = min(configured_page_size, max_records)
        contract = TABLE_CONTRACTS["vendors"]
        target = continuation or contract.endpoint
        request_params: dict[str, str] | None = None
        if continuation is None:
            request_params = dict(contract.default_params)
            request_params["page_size"] = str(effective_page_size)
            if phase == "incremental":
                if window_start is None:
                    raise AssertionError("Incremental vendor window requires a lower bound")
                request_params[cast(str, contract.cursor_filter)] = window_start
            request_params[cast(str, contract.upper_cursor_filter)] = window_end

        extracted_at = datetime.now(timezone.utc).isoformat()
        rows_by_id: dict[str, dict[str, Any]] = {}
        while True:
            page, next_url = self._api.get_list_page(target, params=request_params)
            request_params = None
            page_count += 1
            if page_count > MAX_PAGES_PER_TRIGGER:
                raise RampApiError("Ramp pagination exceeded its bounded page limit")
            if len(page) > effective_page_size:
                raise RampApiError("Ramp page exceeded the requested page_size")

            for record in page:
                record_id = self._required_record_id("vendors", record)
                rows_by_id[record_id] = self._normalize_record(
                    "vendors",
                    record,
                    tenant_id=tenant_id,
                    extracted_at=extracted_at,
                    window_end=window_end,
                )

            if next_url is None:
                return list(rows_by_id.values()), self._vendor_window_complete_offset(
                    tenant_id=tenant_id,
                    phase=phase,
                    upper_bound=upper_bound,
                    window_end=window_end,
                )
            if next_url in (target, previous_continuation):
                raise RampApiError("Ramp pagination returned a repeated continuation URL")

            if len(rows_by_id) + effective_page_size > max_records:
                partial = self._base_offset(
                    tenant_id=tenant_id,
                    phase=phase,
                    upper_bound=upper_bound,
                )
                partial["continuation"] = next_url
                partial["page_count"] = page_count
                if target.startswith("https://"):
                    partial["previous_continuation"] = target
                if phase == "incremental":
                    if committed_cursor is None or window_start is None:
                        raise AssertionError("Incremental vendor page lost its window state")
                    partial.update(
                        {
                            "cursor": committed_cursor,
                            "window_start": window_start,
                            "window_end": window_end,
                            "lookback_applied": True,
                        }
                    )
                return list(rows_by_id.values()), partial

            previous_continuation = target if target.startswith("https://") else None
            target = next_url

    def _vendor_window_complete_offset(
        self,
        *,
        tenant_id: str,
        phase: Literal["initial", "incremental"],
        upper_bound: str,
        window_end: str,
    ) -> RampOffset:
        """Commit a fully drained initial snapshot or update window."""
        if phase == "initial" or self._parse_timestamp(
            "window_end", window_end
        ) >= self._parse_timestamp("upper_bound", upper_bound):
            return self._complete_offset(tenant_id, upper_bound)

        offset = self._base_offset(
            tenant_id=tenant_id,
            phase="incremental",
            upper_bound=upper_bound,
        )
        offset["cursor"] = window_end
        offset["lookback_applied"] = True
        return offset

    def _complete_offset(self, tenant_id: str, cursor: str) -> RampOffset:
        offset = self._base_offset(
            tenant_id=tenant_id,
            phase="complete",
            upper_bound=cursor,
        )
        offset["cursor"] = cursor
        return offset

    def _base_offset(
        self,
        *,
        tenant_id: str,
        phase: Literal["initial", "incremental", "complete"],
        upper_bound: str,
    ) -> RampOffset:
        return {
            "version": OFFSET_VERSION,
            "table": "vendors",
            "tenant_id": tenant_id,
            "environment": self._api.environment,
            "flow": "records",
            "phase": phase,
            "upper_bound": upper_bound,
        }

    def _read_complete_snapshot(
        self,
        table_name: str,
        tenant_id: str,
        table_options: dict[str, str],
    ) -> list[dict[str, Any]]:
        contract = TABLE_CONTRACTS[table_name]
        params = dict(contract.default_params)
        params["page_size"] = table_options.get("page_size", str(DEFAULT_PAGE_SIZE))
        request_params = [params]
        if contract.fanout:
            request_params = []
            for key, value in contract.fanout:
                request_params.append({**params, key: value})

        extracted_at = datetime.now(timezone.utc).isoformat()
        rows_by_id: dict[str, dict[str, Any]] = {}
        for scoped_params in request_params:
            for record in self._api.iter_list(
                contract.endpoint,
                params=scoped_params,
            ):
                record_id = self._required_record_id(table_name, record)
                rows_by_id[record_id] = self._normalize_record(
                    table_name,
                    record,
                    tenant_id=tenant_id,
                    extracted_at=extracted_at,
                    window_end=None,
                )
        return list(rows_by_id.values())

    @staticmethod
    def _required_record_id(table_name: str, record: dict[str, Any]) -> str:
        record_id = record.get("id") if isinstance(record, dict) else None
        if not isinstance(record_id, str) or not record_id.strip():
            raise RampApiError(f"Ramp {table_name} record is missing required id")
        return record_id

    def _normalize_record(
        self,
        table_name: str,
        record: dict[str, Any],
        *,
        tenant_id: str,
        extracted_at: str,
        window_end: str | None,
    ) -> dict[str, Any]:
        schema = TABLE_SCHEMAS[table_name]
        json_fields = JSON_STRING_FIELDS[table_name]
        row: dict[str, Any] = {}

        for field in schema.fields:
            name = field.name
            if name == "tenant_id":
                row[name] = tenant_id
                continue
            if name == "_ramp_extracted_at":
                row[name] = extracted_at
                continue
            if name == "_ramp_window_end":
                row[name] = window_end
                continue

            value = record.get(name)
            if name in json_fields and value is not None and not isinstance(value, str):
                value = self._canonical_json(value)
            elif isinstance(field.dataType, StructType) and value == {}:
                value = None
            row[name] = value

        payment_method = row.get("default_payment_method")
        if isinstance(payment_method, dict):
            normalized_method = dict(payment_method)
            policy = normalized_method.get("policy")
            if policy is not None and not isinstance(policy, str):
                normalized_method["policy"] = self._canonical_json(policy)
            row["default_payment_method"] = normalized_method
        return row

    @staticmethod
    def _canonical_json(value: Any) -> str:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=RampLakeflowConnect._json_default,
        )

    @staticmethod
    def _json_default(value: Any) -> str:
        if isinstance(value, Decimal):
            return str(value)
        raise TypeError(f"Unsupported canonical JSON value: {type(value).__name__}")

    def _validate_offset(
        self,
        table_name: str,
        tenant_id: str,
        offset: dict,
    ) -> RampOffset:
        if not isinstance(offset, dict):
            raise ValueError("Ramp offset must be a dictionary")
        if offset.get("table") != table_name:
            raise ValueError("Ramp offset table does not match the requested table")
        if offset.get("tenant_id") != tenant_id:
            raise ValueError("Ramp offset tenant does not match the authenticated business")
        if offset.get("environment") != self._api.environment:
            raise ValueError("Ramp offset environment does not match the connection")
        if offset.get("version") != OFFSET_VERSION:
            raise ValueError(f"Unsupported Ramp offset version; expected {OFFSET_VERSION}")
        if offset.get("flow") != "records":
            raise ValueError("Ramp offset flow must be records")
        phase = offset.get("phase")
        if phase not in {"initial", "incremental", "complete"}:
            raise ValueError("Ramp offset phase is invalid")

        upper_bound = self._required_offset_timestamp(offset, "upper_bound")
        self._validate_offset_phase(offset, phase, upper_bound)

        if "page_count" in offset:
            page_count = offset["page_count"]
            if not isinstance(page_count, int) or isinstance(page_count, bool) or page_count < 0:
                raise ValueError("Ramp offset page_count must be a non-negative integer")
            if page_count >= MAX_PAGES_PER_TRIGGER:
                raise ValueError("Ramp offset page_count exceeds the bounded page limit")
        if "previous_continuation" in offset:
            self._required_offset_string(offset, "previous_continuation")
        return cast(RampOffset, dict(offset))

    @classmethod
    def _validate_offset_phase(
        cls,
        offset: dict,
        phase: str,
        upper_bound: datetime,
    ) -> None:
        """Validate fields whose meaning depends on the checkpoint phase."""
        if phase == "initial":
            cls._required_offset_string(offset, "continuation")
        else:
            cursor = cls._required_offset_timestamp(offset, "cursor")
            if cursor > upper_bound:
                raise ValueError("Ramp offset cursor cannot exceed upper_bound")
            if phase == "complete" and cursor != upper_bound:
                raise ValueError("Ramp complete offset cursor must equal upper_bound")
            if phase == "incremental":
                if cursor >= upper_bound:
                    raise ValueError("Ramp incremental offset cursor must precede upper_bound")
                if offset.get("lookback_applied") is not True:
                    raise ValueError("Ramp incremental offset must record lookback_applied")
                if "continuation" in offset:
                    cls._required_offset_string(offset, "continuation")
                    window_start = cls._required_offset_timestamp(offset, "window_start")
                    window_end = cls._required_offset_timestamp(offset, "window_end")
                    if window_start > window_end:
                        raise ValueError("Ramp offset window_start cannot exceed window_end")
                    if window_end > upper_bound or window_end <= cursor:
                        raise ValueError("Ramp offset window_end is outside the active bounds")

    @staticmethod
    def _required_offset_string(offset: dict, field: str) -> str:
        value = offset.get(field)
        if not isinstance(value, str) or not value:
            raise ValueError(f"Ramp offset {field} must be a non-empty string")
        return value

    @classmethod
    def _required_offset_timestamp(cls, offset: dict, field: str) -> datetime:
        return cls._parse_timestamp(field, cls._required_offset_string(offset, field))

    @staticmethod
    def _validate_table(table_name: str) -> None:
        if table_name not in TABLE_SCHEMAS:
            supported = ", ".join(SUPPORTED_TABLES)
            raise ValueError(
                f"Unsupported Ramp table {table_name!r}. Supported tables: {supported}"
            )

    @staticmethod
    def _validate_table_options(table_name: str, table_options: dict[str, str]) -> None:
        allowed = TABLE_OPTION_ALLOWLIST[table_name]
        is_managed_runtime = bool(MANAGED_RUNTIME_OPTION_MARKERS & set(table_options))
        unknown = [] if is_managed_runtime else sorted(set(table_options) - allowed)
        if unknown:
            raise ValueError(
                f"Unsupported options for Ramp table {table_name!r}: {unknown}. "
                f"Allowed options: {sorted(allowed)}"
            )

        if "page_size" in allowed and "page_size" in table_options:
            RampLakeflowConnect._validate_int_option(
                "page_size",
                table_options["page_size"],
                minimum=2,
                maximum=MAX_PAGE_SIZE,
            )
        if "max_records_per_batch" in allowed and "max_records_per_batch" in table_options:
            RampLakeflowConnect._validate_int_option(
                "max_records_per_batch",
                table_options["max_records_per_batch"],
                minimum=2,
            )
        if "window_seconds" in allowed and "window_seconds" in table_options:
            RampLakeflowConnect._validate_int_option(
                "window_seconds", table_options["window_seconds"], minimum=1
            )
        if "lookback_seconds" in allowed and "lookback_seconds" in table_options:
            RampLakeflowConnect._validate_int_option(
                "lookback_seconds",
                table_options["lookback_seconds"],
                minimum=0,
            )

    @staticmethod
    def _option_int(table_options: dict[str, str], name: str, default: int) -> int:
        return int(table_options.get(name, str(default)))

    @staticmethod
    def _validate_int_option(
        name: str,
        value: str,
        *,
        minimum: int,
        maximum: int | None = None,
    ) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Ramp table option {name!r} must be an integer") from exc

        if parsed < minimum or (maximum is not None and parsed > maximum):
            upper = f" and {maximum}" if maximum is not None else ""
            raise ValueError(f"Ramp table option {name!r} must be between {minimum}{upper}")
        return parsed

    @staticmethod
    def _validate_timestamp(name: str, value: str) -> datetime:
        return RampLakeflowConnect._parse_timestamp(f"table option {name!r}", value)

    @staticmethod
    def _parse_timestamp(name: str, value: str) -> datetime:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (AttributeError, ValueError) as exc:
            raise ValueError(f"Ramp {name} must be an ISO 8601 timestamp") from exc
        if parsed.tzinfo is None:
            raise ValueError(f"Ramp {name} must include a UTC offset")
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _format_timestamp(value: datetime) -> str:
        return value.astimezone(timezone.utc).isoformat()
