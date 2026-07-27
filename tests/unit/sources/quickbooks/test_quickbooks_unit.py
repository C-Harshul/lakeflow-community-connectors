"""Focused failure-mode and normalization tests for QuickBooks Online."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import Mock

import pytest
import requests

from databricks.labs.community_connector.sources.quickbooks import quickbooks as quickbooks_module
from databricks.labs.community_connector.sources.quickbooks.quickbooks import (
    QuickBooksApiClient,
    QuickBooksLakeflowConnect,
    _normalize_entity,
    _retry_after_seconds,
)


def _options(**overrides: str) -> dict[str, str]:
    return {
        "access_token": "token",
        "realm_id": "realm",
        "environment": "sandbox",
        **overrides,
    }


def _response(status: int, payload: object | None = None, **headers: str) -> Mock:
    response = Mock(spec=requests.Response)
    response.status_code = status
    response.headers = headers
    response.json.return_value = payload
    if status >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(str(status))
    else:
        response.raise_for_status.return_value = None
    return response


@pytest.mark.parametrize("missing", ["access_token", "realm_id"])
def test_required_connection_values(missing: str) -> None:
    options = _options()
    options[missing] = " "
    with pytest.raises(ValueError, match=missing):
        QuickBooksLakeflowConnect(options)


def test_invalid_environment() -> None:
    with pytest.raises(ValueError, match="environment"):
        QuickBooksLakeflowConnect(_options(environment="staging"))


def test_table_discovery_and_specialized_schemas() -> None:
    connector = QuickBooksLakeflowConnect(_options())
    assert connector.list_tables() == [
        "customers",
        "vendors",
        "accounts",
        "items",
        "invoices",
        "bills",
    ]
    assert "primary_email" in connector.get_table_schema("customers", {}).fieldNames()
    assert "account_type" in connector.get_table_schema("accounts", {}).fieldNames()
    assert "quantity_on_hand" in connector.get_table_schema("items", {}).fieldNames()
    assert "customer_ref" in connector.get_table_schema("invoices", {}).fieldNames()
    assert "vendor_ref" in connector.get_table_schema("bills", {}).fieldNames()


def test_customer_metadata_is_cdc_while_other_tables_remain_snapshots() -> None:
    connector = QuickBooksLakeflowConnect(_options())

    assert connector.read_table_metadata("customers", {}) == {
        "primary_keys": ["id"],
        "cursor_field": "last_updated_at",
        "ingestion_type": "cdc",
    }
    assert connector.read_table_metadata("vendors", {}) == {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    }


def test_customer_first_read_is_snapshot_with_versioned_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    init_time = datetime(2026, 7, 26, 12, 30, tzinfo=timezone.utc)
    monkeypatch.setattr(quickbooks_module, "_utc_now", lambda: init_time)
    get = Mock(
        return_value=_response(
            200,
            {
                "QueryResponse": {
                    "Customer": [
                        {
                            "Id": "1",
                            "MetaData": {
                                "LastUpdatedTime": "2026-07-26T12:00:00Z",
                            },
                        }
                    ]
                }
            },
        )
    )
    monkeypatch.setattr(requests, "get", get)
    connector = QuickBooksLakeflowConnect(_options())

    records, end_offset = connector.read_table("customers", {}, {"page_size": "10"})

    assert [record["id"] for record in records] == ["1"]
    assert end_offset == {
        "version": 1,
        "updated_through": "2026-07-26T12:30:00Z",
    }
    assert " WHERE " not in get.call_args.kwargs["params"]["query"]

    records, repeated_offset = connector.read_table(
        "customers",
        end_offset,
        {"page_size": "10"},
    )
    assert list(records) == []
    assert repeated_offset == end_offset
    assert get.call_count == 1


def test_customer_incremental_read_uses_overlap_and_bounded_upper_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        quickbooks_module,
        "_utc_now",
        lambda: datetime(2026, 7, 26, 12, 30, tzinfo=timezone.utc),
    )
    get = Mock(
        return_value=_response(
            200,
            {
                "QueryResponse": {
                    "Customer": [
                        {
                            "Id": "2",
                            "MetaData": {
                                "LastUpdatedTime": "2026-07-25T12:00:00Z",
                            },
                        }
                    ]
                }
            },
        )
    )
    monkeypatch.setattr(requests, "get", get)
    connector = QuickBooksLakeflowConnect(_options())
    start_offset = {
        "version": 1,
        "updated_through": "2026-07-25T11:30:00Z",
    }

    records, end_offset = connector.read_table(
        "customers",
        start_offset,
        {
            "page_size": "10",
            "incremental_overlap_seconds": "60",
            "max_incremental_window_seconds": "3600",
        },
    )

    assert [record["id"] for record in records] == ["2"]
    assert end_offset == {
        "version": 1,
        "updated_through": "2026-07-25T12:30:00Z",
    }
    query = get.call_args.kwargs["params"]["query"]
    assert "MetaData.LastUpdatedTime >= '2026-07-25T11:29:00Z'" in query
    assert "MetaData.LastUpdatedTime <= '2026-07-25T12:30:00Z'" in query


def test_customer_incremental_replay_is_deterministic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        quickbooks_module,
        "_utc_now",
        lambda: datetime(2026, 7, 26, 12, 30, tzinfo=timezone.utc),
    )
    response = _response(200, {"QueryResponse": {"Customer": []}})
    get = Mock(return_value=response)
    monkeypatch.setattr(requests, "get", get)
    connector = QuickBooksLakeflowConnect(_options())
    start_offset = {
        "version": 1,
        "updated_through": "2026-07-26T10:30:00Z",
    }

    first_records, first_offset = connector.read_table("customers", start_offset, {})
    second_records, second_offset = connector.read_table("customers", start_offset, {})

    assert list(first_records) == []
    assert list(second_records) == []
    assert first_offset == second_offset
    assert (
        get.call_args_list[0].kwargs["params"]["query"]
        == (get.call_args_list[1].kwargs["params"]["query"])
    )


def test_customer_incremental_failure_replays_from_same_offset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        quickbooks_module,
        "_utc_now",
        lambda: datetime(2026, 7, 26, 12, 30, tzinfo=timezone.utc),
    )
    get = Mock(
        side_effect=[
            _response(200, {"QueryResponse": {"Customer": [{"Id": "missing-metadata"}]}}),
            _response(200, {"QueryResponse": {"Customer": []}}),
        ]
    )
    monkeypatch.setattr(requests, "get", get)
    connector = QuickBooksLakeflowConnect(_options())
    start_offset = {
        "version": 1,
        "updated_through": "2026-07-26T10:30:00Z",
    }

    failed_records, failed_end_offset = connector.read_table("customers", start_offset, {})
    with pytest.raises(RuntimeError, match="LastUpdatedTime"):
        list(failed_records)

    replayed_records, replayed_end_offset = connector.read_table("customers", start_offset, {})

    assert list(replayed_records) == []
    assert failed_end_offset == replayed_end_offset
    assert (
        get.call_args_list[0].kwargs["params"]["query"]
        == get.call_args_list[1].kwargs["params"]["query"]
    )


@pytest.mark.parametrize(
    "offset,match",
    [
        ({"updated_through": "2026-07-26T10:00:00Z"}, "version"),
        ({"version": 2, "updated_through": "2026-07-26T10:00:00Z"}, "version"),
        ({"version": 1}, "updated_through"),
        ({"version": 1, "updated_through": "not-a-time"}, "timestamp"),
    ],
)
def test_customer_offset_validation(offset: dict, match: str) -> None:
    connector = QuickBooksLakeflowConnect(_options())

    with pytest.raises(ValueError, match=match):
        connector.read_table("customers", offset, {})


def test_customer_cdc_requires_last_updated_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get = Mock(
        return_value=_response(
            200,
            {"QueryResponse": {"Customer": [{"Id": "1"}]}},
        )
    )
    monkeypatch.setattr(requests, "get", get)
    connector = QuickBooksLakeflowConnect(_options())

    records, _ = connector.read_table("customers", {}, {})

    with pytest.raises(RuntimeError, match="LastUpdatedTime"):
        list(records)


@pytest.mark.parametrize(
    "options,match",
    [
        ({"incremental_overlap_seconds": "-1"}, "incremental_overlap_seconds"),
        ({"incremental_overlap_seconds": "bad"}, "incremental_overlap_seconds"),
        ({"max_incremental_window_seconds": "59"}, "max_incremental_window_seconds"),
        ({"max_incremental_window_seconds": "bad"}, "max_incremental_window_seconds"),
    ],
)
def test_customer_incremental_option_validation(
    options: dict[str, str],
    match: str,
) -> None:
    connector = QuickBooksLakeflowConnect(_options())
    start_offset = {
        "version": 1,
        "updated_through": "2026-07-20T10:00:00Z",
    }

    with pytest.raises(ValueError, match=match):
        connector.read_table("customers", start_offset, options)


def test_pagination_requests_empty_page_after_exact_full_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = [
        _response(200, {"QueryResponse": {"Customer": [{"Id": "1"}, {"Id": "2"}]}}),
        _response(200, {"QueryResponse": {"Customer": []}}),
    ]
    get = Mock(side_effect=responses)
    monkeypatch.setattr(requests, "get", get)
    client = QuickBooksApiClient(
        access_token="token",
        realm_id="realm",
        environment="sandbox",
        minor_version=75,
    )

    assert [row["Id"] for row in client.iter_entity("Customer", page_size=2)] == ["1", "2"]
    assert get.call_count == 2
    assert "STARTPOSITION 1 MAXRESULTS 2" in get.call_args_list[0].kwargs["params"]["query"]
    assert "STARTPOSITION 3 MAXRESULTS 2" in get.call_args_list[1].kwargs["params"]["query"]


def test_incremental_pagination_preserves_where_clause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = [
        _response(200, {"QueryResponse": {"Customer": [{"Id": "1"}, {"Id": "2"}]}}),
        _response(200, {"QueryResponse": {"Customer": [{"Id": "3"}]}}),
    ]
    get = Mock(side_effect=responses)
    monkeypatch.setattr(requests, "get", get)
    client = QuickBooksApiClient(
        access_token="token",
        realm_id="realm",
        environment="sandbox",
        minor_version=75,
    )
    where_clause = (
        "MetaData.LastUpdatedTime >= '2026-07-25T10:00:00Z' "
        "AND MetaData.LastUpdatedTime <= '2026-07-26T10:00:00Z'"
    )

    rows = list(
        client.iter_entity(
            "Customer",
            page_size=2,
            where_clause=where_clause,
        )
    )

    assert [row["Id"] for row in rows] == ["1", "2", "3"]
    for call in get.call_args_list:
        assert f"WHERE {where_clause}" in call.kwargs["params"]["query"]
    assert "STARTPOSITION 1 MAXRESULTS 2" in get.call_args_list[0].kwargs["params"]["query"]
    assert "STARTPOSITION 3 MAXRESULTS 2" in get.call_args_list[1].kwargs["params"]["query"]


@pytest.mark.parametrize("status", [401, 403])
def test_auth_failures_are_not_retried(monkeypatch: pytest.MonkeyPatch, status: int) -> None:
    get = Mock(return_value=_response(status))
    monkeypatch.setattr(requests, "get", get)
    client = QuickBooksApiClient(
        access_token="token",
        realm_id="realm",
        environment="production",
        minor_version=75,
    )
    with pytest.raises(PermissionError, match="authentication failed"):
        list(client.iter_entity("Customer", page_size=10))
    assert get.call_count == 1


def test_retry_after_is_honored(monkeypatch: pytest.MonkeyPatch) -> None:
    get = Mock(
        side_effect=[
            _response(429, None, **{"Retry-After": "2"}),
            _response(200, {"QueryResponse": {"Customer": []}}),
        ]
    )
    sleep = Mock()
    monkeypatch.setattr(requests, "get", get)
    monkeypatch.setattr("time.sleep", sleep)
    client = QuickBooksApiClient(
        access_token="token",
        realm_id="realm",
        environment="sandbox",
        minor_version=75,
    )
    assert list(client.iter_entity("Customer", page_size=10)) == []
    sleep.assert_called_once_with(2.0)


def test_transient_failure_exhaustion(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(requests, "get", Mock(return_value=_response(503)))
    monkeypatch.setattr("time.sleep", Mock())
    client = QuickBooksApiClient(
        access_token="token",
        realm_id="realm",
        environment="sandbox",
        minor_version=75,
        max_retries=2,
    )
    with pytest.raises(RuntimeError, match="retry exhaustion"):
        list(client.iter_entity("Customer", page_size=10))


def test_network_failure_exhaustion(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(requests, "get", Mock(side_effect=requests.ConnectionError("offline")))
    monkeypatch.setattr("time.sleep", Mock())
    client = QuickBooksApiClient(
        access_token="token",
        realm_id="realm",
        environment="sandbox",
        minor_version=75,
        max_retries=2,
    )
    with pytest.raises(RuntimeError, match="retry exhaustion"):
        list(client.iter_entity("Customer", page_size=10))


def test_invalid_json(monkeypatch: pytest.MonkeyPatch) -> None:
    response = _response(200)
    response.json.side_effect = ValueError("bad json")
    monkeypatch.setattr(requests, "get", Mock(return_value=response))
    client = QuickBooksApiClient(
        access_token="token",
        realm_id="realm",
        environment="sandbox",
        minor_version=75,
    )
    with pytest.raises(RuntimeError, match="invalid JSON"):
        list(client.iter_entity("Customer", page_size=10))


def test_customer_decimal_timestamp_and_raw_payload() -> None:
    record = _normalize_entity(
        "customers",
        {
            "Id": "1",
            "Balance": "12.340",
            "Active": False,
            "DisplayName": "Example",
            "MetaData": {
                "CreateTime": "2026-07-20T10:00:00Z",
                "LastUpdatedTime": "2026-07-21T11:30:00+00:00",
            },
        },
    )
    assert record["balance"] == Decimal("12.340")
    assert record["created_at"] == datetime.fromisoformat("2026-07-20T10:00:00+00:00")
    assert record["active"] is False
    assert '"Id":"1"' in record["raw_json"]


def test_transaction_dates_and_lines() -> None:
    record = _normalize_entity(
        "invoices",
        {
            "Id": "40",
            "TxnDate": "2026-07-01",
            "DueDate": "2026-07-31",
            "TotalAmt": 10.25,
            "Line": [{"Id": "1"}],
        },
    )
    assert record["txn_date"] == "2026-07-01"
    assert record["due_date"] == "2026-07-31"
    assert record["total_amount"] == Decimal("10.25")
    assert record["line_json"] == '[{"Id":"1"}]'


@pytest.mark.parametrize("value,expected", [(None, None), ("", None), ("3", 3.0), ("0", 0.0)])
def test_retry_after_seconds(value: str | None, expected: float | None) -> None:
    assert _retry_after_seconds(value) == expected
