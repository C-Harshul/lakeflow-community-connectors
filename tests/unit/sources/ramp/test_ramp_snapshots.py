"""Focused tenant, fan-out, normalization, and initial-offset tests."""

import json
from collections.abc import Iterable
from typing import Any

import pytest

from databricks.labs.community_connector.sources.ramp.ramp import (
    OFFSET_VERSION,
    RampApiError,
    RampLakeflowConnect,
)

TENANT_ID = "00000000-0000-4000-8000-000000000001"


class StubApiClient:
    """In-memory Ramp API boundary for connector-only tests."""

    environment = "sandbox"

    def __init__(self, records: dict[str, list[dict[str, Any]]]) -> None:
        """Store endpoint records and initialize the request log."""
        self.records = records
        self.calls: list[tuple[str, dict[str, str]]] = []

    def get_business_id(self) -> str:
        """Return the fixed simulator tenant."""
        return TENANT_ID

    def iter_list(
        self, endpoint: str, *, params: dict[str, str] | None = None
    ) -> Iterable[dict[str, Any]]:
        """Yield records matching completeness fan-out parameters."""
        actual_params = dict(params or {})
        self.calls.append((endpoint, actual_params))
        records = self.records.get(endpoint, [])
        for record in records:
            if (
                "direction" in actual_params
                and record.get("direction") != actual_params["direction"]
            ):
                continue
            if "status" in actual_params and record.get("status") != actual_params["status"]:
                continue
            yield dict(record)

    def get_list_page(
        self, endpoint: str, *, params: dict[str, str] | None = None
    ) -> tuple[list[dict[str, Any]], None]:
        """Return one terminal page for focused vendor tests."""
        actual_params = dict(params or {})
        self.calls.append((endpoint, actual_params))
        return [dict(record) for record in self.records.get(endpoint, [])], None


def connector_with(
    records: dict[str, list[dict[str, Any]]],
) -> tuple[RampLakeflowConnect, StubApiClient]:
    """Build a connector with an injected no-network API boundary."""
    api = StubApiClient(records)
    connector = RampLakeflowConnect(
        {
            "environment": "sandbox",
            "access_token": "simulator-fake-access-token",
        },
        api_client=api,
    )
    return connector, api


def test_snapshot_table_enriches_tenant_and_returns_none_offset() -> None:
    """Reference dimensions are complete snapshots with tenant metadata."""
    connector, _ = connector_with(
        {"/developer/v1/departments": [{"id": "department-1", "name": "Finance"}]}
    )

    records, offset = connector.read_table("departments", {}, {"page_size": "5"})
    rows = list(records)

    assert offset is None
    assert rows[0]["tenant_id"] == TENANT_ID
    assert rows[0]["_ramp_extracted_at"].endswith("+00:00")
    assert rows[0]["id"] == "department-1"


def test_batch_reader_none_offset_starts_initial_snapshot() -> None:
    """The framework's batch path uses None for the initial snapshot offset."""
    connector, _ = connector_with(
        {"/developer/v1/departments": [{"id": "department-1", "name": "Finance"}]}
    )

    records, offset = connector.read_table("departments", None, {})

    assert [row["id"] for row in records] == ["department-1"]
    assert offset is None


def test_downgraded_transaction_snapshot_returns_none_offset() -> None:
    """Transactions stay lossless by avoiding an unprovable CDC checkpoint."""
    connector, api = connector_with(
        {
            "/developer/v1/transactions": [
                {
                    "id": "transaction-1",
                    "synced_at": "2026-01-01T00:00:00Z",
                    "amount": "12.34",
                }
            ]
        }
    )

    records, offset = connector.read_table("transactions", {}, {})
    rows = list(records)

    assert len(rows) == 1
    assert offset is None
    assert api.calls == [("/developer/v1/transactions", {"state": "ALL", "page_size": "100"})]


def test_cross_tenant_or_cross_table_offset_is_rejected() -> None:
    """An offset cannot be replayed for another tenant or resource."""
    connector, _ = connector_with({})
    base = {
        "version": OFFSET_VERSION,
        "table": "vendors",
        "tenant_id": TENANT_ID,
        "environment": "sandbox",
        "flow": "records",
        "phase": "complete",
        "cursor": "2026-01-01T00:00:00+00:00",
        "upper_bound": "2026-01-01T00:00:00+00:00",
    }

    with pytest.raises(ValueError, match="tenant"):
        connector.read_table("vendors", {**base, "tenant_id": "other"}, {})
    with pytest.raises(ValueError, match="table"):
        connector.read_table("vendors", {**base, "table": "transactions"}, {})


def test_reimbursement_direction_fanout_is_complete_and_deduplicated() -> None:
    """Both direction requests complete before duplicate IDs are emitted."""
    endpoint = "/developer/v1/reimbursements"
    connector, api = connector_with(
        {
            endpoint: [
                {
                    "id": "reimbursement-1",
                    "direction": "BUSINESS_TO_USER",
                    "updated_at": "2026-01-01T00:00:00Z",
                },
                {
                    "id": "reimbursement-2",
                    "direction": "USER_TO_BUSINESS",
                    "updated_at": "2026-01-02T00:00:00Z",
                },
            ]
        }
    )

    records, _ = connector.read_table("reimbursements", {}, {})

    assert {row["id"] for row in records} == {
        "reimbursement-1",
        "reimbursement-2",
    }
    assert api.calls == [
        (endpoint, {"direction": "BUSINESS_TO_USER", "page_size": "100"}),
        (endpoint, {"direction": "USER_TO_BUSINESS", "page_size": "100"}),
    ]


def test_user_status_fanout_is_complete() -> None:
    """The user snapshot requests every documented status value."""
    endpoint = "/developer/v1/users"
    statuses = ["USER_ACTIVE", "USER_DRAFT", "USER_INACTIVE", "USER_SUSPENDED"]
    connector, api = connector_with(
        {
            endpoint: [
                {"id": f"user-{index}", "status": status} for index, status in enumerate(statuses)
            ]
        }
    )

    records, offset = connector.read_table("users", {}, {})

    assert offset is None
    assert {row["status"] for row in records} == set(statuses)
    assert [params["status"] for _, params in api.calls] == statuses


def test_normalization_adds_missing_fields_and_canonical_json() -> None:
    """Rows match the static schema and dynamic structures serialize stably."""
    connector, _ = connector_with(
        {
            "/developer/v1/transactions": [
                {
                    "id": "transaction-1",
                    "synced_at": "2026-01-01T00:00:00Z",
                    "accounting_field_selections": [{"z": 1, "a": 2}],
                }
            ]
        }
    )

    records, _ = connector.read_table("transactions", {}, {})
    row = next(records)

    assert row["accounting_field_selections"] == '[{"a":2,"z":1}]'
    assert row["memo"] is None
    assert set(row) == set(connector.get_table_schema("transactions", {}).fieldNames())


def test_vendor_policy_is_canonical_json_inside_typed_struct() -> None:
    """The polymorphic payment policy is serialized without flattening it."""
    connector, _ = connector_with(
        {
            "/developer/v1/vendors": [
                {
                    "id": "vendor-1",
                    "default_payment_method": {
                        "update_source": "AUTO",
                        "policy": {"kind": "CARD", "card_id": "card-1"},
                    },
                }
            ]
        }
    )

    records, _ = connector.read_table("vendors", {}, {})
    row = next(records)

    assert row["default_payment_method"] == {
        "update_source": "AUTO",
        "policy": '{"card_id":"card-1","kind":"CARD"}',
    }
    assert row["_ramp_window_end"] == connector.initial_snapshot_upper_bound


def test_missing_or_invalid_record_id_fails_safely() -> None:
    """A source row without a usable primary key cannot be emitted."""
    connector, _ = connector_with({"/developer/v1/departments": [{"name": "Finance"}]})

    with pytest.raises(RampApiError, match="missing required id"):
        connector.read_table("departments", {}, {})


def test_dynamic_json_is_valid_json() -> None:
    """Canonical JSON columns remain machine-readable downstream."""
    connector, _ = connector_with(
        {
            "/developer/v1/users": [
                {
                    "id": "user-1",
                    "status": "USER_ACTIVE",
                    "custom_fields": [{"name": "Region", "value": "APAC"}],
                }
            ]
        }
    )

    records, _ = connector.read_table("users", {}, {})
    row = next(records)
    assert json.loads(row["custom_fields"]) == [{"name": "Region", "value": "APAC"}]


def test_already_serialized_dynamic_json_is_not_double_encoded() -> None:
    """An API string value is preserved instead of becoming a quoted JSON string."""
    connector, _ = connector_with(
        {
            "/developer/v1/users": [
                {
                    "id": "user-1",
                    "status": "USER_ACTIVE",
                    "custom_fields": '[{"name":"Region","value":"APAC"}]',
                }
            ]
        }
    )

    records, _ = connector.read_table("users", {}, {})
    row = next(records)
    assert row["custom_fields"] == '[{"name":"Region","value":"APAC"}]'
