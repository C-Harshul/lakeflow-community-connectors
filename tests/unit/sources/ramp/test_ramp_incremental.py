"""M4 lossless bounded-window and resumable-pagination tests for Ramp."""

from datetime import datetime, timezone
from typing import Any

import pytest

from databricks.labs.community_connector.sources.ramp.ramp import (
    OFFSET_VERSION,
    RampApiError,
    RampLakeflowConnect,
)

TENANT_ID = "00000000-0000-4000-8000-000000000001"
VENDORS_ENDPOINT = "/developer/v1/vendors"
TARGET = "2026-01-03T00:00:00+00:00"


class PageApiClient:
    """Deterministic page-level API boundary for offset state-machine tests."""

    environment = "sandbox"

    def __init__(
        self,
        pages: dict[str, tuple[list[dict[str, Any]], str | None]],
    ) -> None:
        self.pages = pages
        self.calls: list[tuple[str, dict[str, str]]] = []

    def get_business_id(self) -> str:
        return TENANT_ID

    def get_list_page(
        self, target: str, *, params: dict[str, str] | None = None
    ) -> tuple[list[dict[str, Any]], str | None]:
        self.calls.append((target, dict(params or {})))
        records, continuation = self.pages[target]
        return [dict(record) for record in records], continuation


def connector_with_pages(
    pages: dict[str, tuple[list[dict[str, Any]], str | None]],
    *,
    now: str = TARGET,
) -> tuple[RampLakeflowConnect, PageApiClient]:
    api = PageApiClient(pages)
    connector = RampLakeflowConnect(
        {"environment": "sandbox", "access_token": "simulator-fake-access-token"},
        api_client=api,
        now=lambda: datetime.fromisoformat(now),
    )
    return connector, api


def complete_offset(cursor: str = "2026-01-01T00:00:00+00:00") -> dict[str, Any]:
    """Build a valid completed vendor checkpoint from a prior trigger."""
    return {
        "version": OFFSET_VERSION,
        "table": "vendors",
        "tenant_id": TENANT_ID,
        "environment": "sandbox",
        "flow": "records",
        "phase": "complete",
        "cursor": cursor,
        "upper_bound": cursor,
    }


def test_initial_snapshot_resumes_at_page_boundaries_without_loss() -> None:
    """A strict cap under-fills instead of splitting a source page."""
    page_2 = "https://demo-api.ramp.com/developer/v1/vendors?start=vendor-2"
    page_3 = "https://demo-api.ramp.com/developer/v1/vendors?start=vendor-4"
    connector, api = connector_with_pages(
        {
            VENDORS_ENDPOINT: ([{"id": "vendor-1"}, {"id": "vendor-2"}], page_2),
            page_2: ([{"id": "vendor-3"}, {"id": "vendor-4"}], page_3),
            page_3: ([{"id": "vendor-5"}], None),
        }
    )
    options = {"page_size": "2", "max_records_per_batch": "3"}

    rows_1, offset_1 = connector.read_table("vendors", {}, options)
    rows_2, offset_2 = connector.read_table("vendors", offset_1, options)
    rows_3, offset_3 = connector.read_table("vendors", offset_2, options)

    assert [[row["id"] for row in rows] for rows in (rows_1, rows_2, rows_3)] == [
        ["vendor-1", "vendor-2"],
        ["vendor-3", "vendor-4"],
        ["vendor-5"],
    ]
    assert offset_1["phase"] == offset_2["phase"] == "initial"
    assert offset_1["continuation"] == page_2
    assert offset_2["continuation"] == page_3
    assert offset_1["upper_bound"] == offset_2["upper_bound"] == TARGET
    assert offset_3 == complete_offset(TARGET)
    assert api.calls[0] == (
        VENDORS_ENDPOINT,
        {"include_draft": "true", "page_size": "2", "to_updated_at": TARGET},
    )
    assert api.calls[1:] == [(page_2, {}), (page_3, {})]

    calls_before = len(api.calls)
    rows_4, offset_4 = connector.read_table("vendors", offset_3, options)
    assert list(rows_4) == []
    assert offset_4 == offset_3
    assert len(api.calls) == calls_before


def test_incremental_windows_freeze_target_and_apply_lookback_once() -> None:
    """Only the first window in a trigger overlaps the committed cursor."""
    connector, api = connector_with_pages({VENDORS_ENDPOINT: ([], None)})
    options = {
        "page_size": "2",
        "max_records_per_batch": "10",
        "window_seconds": "86400",
        "lookback_seconds": "3600",
    }

    rows_1_iter, offset_1 = connector.read_table("vendors", complete_offset(), options)
    rows_2_iter, offset_2 = connector.read_table("vendors", offset_1, options)
    rows_1 = list(rows_1_iter)
    rows_2 = list(rows_2_iter)

    assert list(rows_1) == list(rows_2) == []
    assert offset_1["phase"] == "incremental"
    assert offset_1["cursor"] == "2026-01-02T00:00:00+00:00"
    assert offset_1["upper_bound"] == TARGET
    assert offset_1["lookback_applied"] is True
    assert offset_2 == complete_offset(TARGET)
    assert api.calls == [
        (
            VENDORS_ENDPOINT,
            {
                "include_draft": "true",
                "page_size": "2",
                "from_updated_at": "2025-12-31T23:00:00+00:00",
                "to_updated_at": "2026-01-02T00:00:00+00:00",
            },
        ),
        (
            VENDORS_ENDPOINT,
            {
                "include_draft": "true",
                "page_size": "2",
                "from_updated_at": "2026-01-02T00:00:00+00:00",
                "to_updated_at": TARGET,
            },
        ),
    ]


def test_incremental_page_checkpoint_keeps_window_bounds() -> None:
    """A resumed page stays inside the original frozen update window."""
    next_url = "https://demo-api.ramp.com/developer/v1/vendors?start=vendor-2"
    connector, api = connector_with_pages(
        {
            VENDORS_ENDPOINT: ([{"id": "vendor-1"}, {"id": "vendor-2"}], next_url),
            next_url: ([{"id": "vendor-3"}], None),
        },
        now="2026-01-02T00:00:00+00:00",
    )
    options = {"page_size": "2", "max_records_per_batch": "3", "lookback_seconds": "0"}

    rows_1_iter, offset_1 = connector.read_table("vendors", complete_offset(), options)
    rows_2_iter, offset_2 = connector.read_table("vendors", offset_1, options)
    rows_1 = list(rows_1_iter)
    rows_2 = list(rows_2_iter)

    assert [row["id"] for row in rows_1] == ["vendor-1", "vendor-2"]
    assert [row["id"] for row in rows_2] == ["vendor-3"]
    assert {row["_ramp_window_end"] for row in [*rows_1, *rows_2]} == {"2026-01-02T00:00:00+00:00"}
    assert offset_1["phase"] == "incremental"
    assert offset_1["cursor"] == "2026-01-01T00:00:00+00:00"
    assert offset_1["window_start"] == "2026-01-01T00:00:00+00:00"
    assert offset_1["window_end"] == "2026-01-02T00:00:00+00:00"
    assert offset_1["continuation"] == next_url
    assert offset_2 == complete_offset("2026-01-02T00:00:00+00:00")
    assert api.calls[1] == (next_url, {})


@pytest.mark.parametrize(
    ("mutated", "message"),
    [
        ({"version": 99}, "version"),
        ({"table": "transactions"}, "table"),
        ({"tenant_id": "other"}, "tenant"),
        ({"environment": "production"}, "environment"),
        ({"flow": "deletes"}, "flow"),
        ({"phase": "unknown"}, "phase"),
        ({"cursor": "not-a-timestamp"}, "cursor"),
    ],
)
def test_invalid_offset_envelopes_are_rejected(mutated: dict[str, Any], message: str) -> None:
    """Offsets are versioned, tenant-bound, phase-checked, and time-checked."""
    connector, _ = connector_with_pages({VENDORS_ENDPOINT: ([], None)})

    with pytest.raises(ValueError, match=message):
        connector.read_table("vendors", {**complete_offset(), **mutated}, {})


def test_non_dictionary_offset_is_rejected_before_tenant_lookup() -> None:
    """A falsey malformed offset cannot be mistaken for a first read."""
    connector, _ = connector_with_pages({VENDORS_ENDPOINT: ([], None)})

    with pytest.raises(ValueError, match="dictionary"):
        connector.read_table("vendors", [], {})  # type: ignore[arg-type]


def test_oversized_api_page_fails_instead_of_breaking_the_batch_cap() -> None:
    """A source that ignores page_size cannot force an over-limit emission."""
    connector, _ = connector_with_pages(
        {VENDORS_ENDPOINT: ([{"id": "1"}, {"id": "2"}, {"id": "3"}], None)}
    )

    with pytest.raises(RampApiError, match="page_size"):
        connector.read_table("vendors", {}, {"page_size": "2", "max_records_per_batch": "2"})


def test_repeated_page_continuation_fails_boundedly() -> None:
    """A continuation that points to itself cannot spin across checkpoints."""
    repeat = "https://demo-api.ramp.com/developer/v1/vendors?start=repeat"
    connector, _ = connector_with_pages(
        {
            VENDORS_ENDPOINT: ([{"id": "vendor-1"}, {"id": "vendor-2"}], repeat),
            repeat: ([{"id": "vendor-3"}], repeat),
        }
    )
    options = {"page_size": "2", "max_records_per_batch": "3"}
    _, offset = connector.read_table("vendors", {}, options)

    with pytest.raises(RampApiError, match="repeated"):
        connector.read_table("vendors", offset, options)


class FailingSecondPageApi(PageApiClient):
    """Fail every continuation request while allowing initial-page retries."""

    def get_list_page(
        self, target: str, *, params: dict[str, str] | None = None
    ) -> tuple[list[dict[str, Any]], str | None]:
        if target.startswith("https://"):
            self.calls.append((target, dict(params or {})))
            raise RampApiError("injected page failure")
        return super().get_list_page(target, params=params)


def test_page_failure_does_not_return_or_advance_a_checkpoint() -> None:
    """Retrying the caller's original offset starts from the same first page."""
    next_url = "https://demo-api.ramp.com/developer/v1/vendors?start=vendor-2"
    api = FailingSecondPageApi(
        {VENDORS_ENDPOINT: ([{"id": "vendor-1"}, {"id": "vendor-2"}], next_url)}
    )
    connector = RampLakeflowConnect(
        {"environment": "sandbox", "access_token": "simulator-fake-access-token"},
        api_client=api,
        now=lambda: datetime(2026, 1, 3, tzinfo=timezone.utc),
    )
    options = {"page_size": "2", "max_records_per_batch": "4"}

    with pytest.raises(RampApiError, match="injected"):
        connector.read_table("vendors", {}, options)
    with pytest.raises(RampApiError, match="injected"):
        connector.read_table("vendors", {}, options)

    assert [target for target, _ in api.calls] == [
        VENDORS_ENDPOINT,
        next_url,
        VENDORS_ENDPOINT,
        next_url,
    ]
