"""Focused HTTP, retry, error, and pagination tests for Ramp."""

import json
from collections.abc import Callable
from typing import Any

import pytest
import requests

from databricks.labs.community_connector.sources.ramp.ramp import (
    RampApiClient,
    RampApiError,
)


class FakeSession:  # pylint: disable=too-few-public-methods
    """Minimal requests.Session stand-in with deterministic outcomes."""

    def __init__(self, outcomes: list[Any]) -> None:
        """Store deterministic responses or exceptions in request order."""
        self.headers: dict[str, str] = {}
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        """Return or raise the next configured outcome."""
        self.calls.append({"method": method, "url": url, **kwargs})
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def response(
    status: int,
    payload: Any,
    *,
    headers: dict[str, str] | None = None,
    url: str = "https://demo-api.ramp.com/developer/v1/transactions",
) -> requests.Response:
    """Build a requests response without making a network call."""
    result = requests.Response()
    result.status_code = status
    result.url = url
    result.headers.update(headers or {})
    result._content = json.dumps(payload).encode("utf-8")  # pylint: disable=protected-access
    result.encoding = "utf-8"
    return result


def make_client(
    outcomes: list[Any],
    *,
    sleep: Callable[[float], None] | None = None,
) -> tuple[RampApiClient, FakeSession]:
    """Construct an injected sandbox client and its fake session."""
    session = FakeSession(outcomes)
    client = RampApiClient(
        {
            "environment": "sandbox",
            "access_token": "simulator-fake-access-token",
        },
        session=session,
        sleep=sleep,
        jitter=lambda: 0.0,
    )
    return client, session


def test_request_uses_bearer_header_and_explicit_timeout() -> None:
    """Every HTTP request carries injected auth and the bounded timeout."""
    client, session = make_client([response(200, {"ok": True})])

    assert client.request_json("GET", "/developer/v1/business") == {"ok": True}
    assert session.headers["Authorization"] == "Bearer simulator-fake-access-token"
    assert session.calls == [
        {
            "method": "GET",
            "url": "https://demo-api.ramp.com/developer/v1/business",
            "params": None,
            "timeout": (10.0, 30.0),
            "allow_redirects": False,
        }
    ]


def test_429_honors_retry_after_before_succeeding() -> None:
    """A numeric Retry-After value overrides exponential backoff."""
    sleeps: list[float] = []
    client, session = make_client(
        [
            response(429, {"message": "slow down"}, headers={"Retry-After": "3"}),
            response(200, {"ok": True}),
        ],
        sleep=sleeps.append,
    )

    assert client.request_json("GET", "/developer/v1/business") == {"ok": True}
    assert sleeps == [3.0]
    assert len(session.calls) == 2


def test_transport_timeout_retries_with_bounded_backoff() -> None:
    """Transient transport failures retry without increasing HTTP timeouts."""
    sleeps: list[float] = []
    client, session = make_client(
        [requests.Timeout("private timeout detail"), response(200, {"ok": True})],
        sleep=sleeps.append,
    )

    assert client.request_json("GET", "/developer/v1/business") == {"ok": True}
    assert sleeps == [1.0]
    assert {call["timeout"] for call in session.calls} == {(10.0, 30.0)}


def test_503_retries_with_exponential_backoff() -> None:
    """Ramp server failures use the same bounded retry policy."""
    sleeps: list[float] = []
    client, session = make_client(
        [response(503, {"message": "unavailable"}), response(200, {"ok": True})],
        sleep=sleeps.append,
    )

    assert client.request_json("GET", "/developer/v1/business") == {"ok": True}
    assert sleeps == [1.0]
    assert len(session.calls) == 2


def test_exhausted_transport_retries_hide_exception_details() -> None:
    """Retry exhaustion is bounded and never echoes transport exception text."""
    sleeps: list[float] = []
    client, session = make_client(
        [requests.Timeout("private-network-detail")] * 5,
        sleep=sleeps.append,
    )

    with pytest.raises(RampApiError) as raised:
        client.request_json("GET", "/developer/v1/business")

    assert "private-network-detail" not in str(raised.value)
    assert sleeps == [1.0, 2.0, 4.0, 8.0]
    assert len(session.calls) == 5


def test_non_retryable_error_is_sanitized() -> None:
    """Errors contain safe Ramp context but never URL queries or credentials."""
    secret_marker = "do-not-leak-this-token"
    client, _ = make_client(
        [
            response(
                403,
                {
                    "error_code": "MISSING_SCOPE",
                    "message": f"denied {secret_marker}",
                    "private": "complete response must not be rendered",
                },
                headers={"x-trace-id": "trace-123"},
                url="https://demo-api.ramp.com/developer/v1/users?email=private",
            )
        ]
    )

    with pytest.raises(RampApiError) as raised:
        client.request_json("GET", "/developer/v1/users")

    rendered = str(raised.value)
    assert "status=403" in rendered
    assert "MISSING_SCOPE" in rendered
    assert "trace-123" in rendered
    assert "?email=" not in rendered
    assert "complete response" not in rendered
    assert secret_marker not in rendered
    assert "simulator-fake-access-token" not in rendered


def test_401_is_terminal_for_uc_managed_token() -> None:
    """The connector does not retry a rejected UC-injected token itself."""
    client, session = make_client([response(401, {"message": "expired"})])

    with pytest.raises(RampApiError, match="status=401"):
        client.request_json("GET", "/developer/v1/business")

    assert len(session.calls) == 1


def test_invalid_json_is_a_sanitized_protocol_error() -> None:
    """A successful non-JSON response fails without echoing its body."""
    bad = response(200, {"unused": True})
    bad._content = b"sensitive non-json body"  # pylint: disable=protected-access
    client, _ = make_client([bad])

    with pytest.raises(RampApiError) as raised:
        client.request_json("GET", "/developer/v1/business")

    assert "valid JSON" in str(raised.value)
    assert "sensitive" not in str(raised.value)


def test_iter_list_follows_valid_next_url_and_terminates() -> None:
    """Ramp page.next is followed verbatim after same-host validation."""
    next_url = "https://demo-api.ramp.com/developer/v1/transactions?page_size=2&start=record-2"
    client, session = make_client(
        [
            response(
                200,
                {"data": [{"id": "record-1"}], "page": {"next": next_url}},
            ),
            response(
                200,
                {"data": [{"id": "record-2"}], "page": {"next": None}},
                url=next_url,
            ),
        ]
    )

    records = list(
        client.iter_list(
            "/developer/v1/transactions",
            params={"state": "ALL", "page_size": "2"},
        )
    )

    assert records == [{"id": "record-1"}, {"id": "record-2"}]
    assert session.calls[0]["params"] == {"state": "ALL", "page_size": "2"}
    assert session.calls[1]["url"] == next_url
    assert session.calls[1]["params"] is None


def test_get_list_page_preserves_the_full_continuation() -> None:
    """The page primitive exposes Ramp's opaque next URL for checkpointing."""
    next_url = "https://demo-api.ramp.com/developer/v1/vendors?start=vendor-1"
    client, session = make_client(
        [response(200, {"data": [{"id": "vendor-1"}], "page": {"next": next_url}})]
    )

    records, continuation = client.get_list_page("/developer/v1/vendors", params={"page_size": "2"})

    assert records == [{"id": "vendor-1"}]
    assert continuation == next_url
    assert session.calls[0]["params"] == {"page_size": "2"}


@pytest.mark.parametrize(
    ("next_url", "message"),
    [
        ("http://demo-api.ramp.com/developer/v1/users?start=1", "HTTPS"),
        ("https://evil.example/developer/v1/users?start=1", "host"),
        ("https://demo-api.ramp.com/private/users?start=1", "path"),
    ],
)
def test_iter_list_rejects_untrusted_continuation_urls(next_url: str, message: str) -> None:
    """Continuation URLs cannot escape the configured Ramp API origin."""
    client, session = make_client([response(200, {"data": [], "page": {"next": next_url}})])

    with pytest.raises(RampApiError, match=message):
        list(client.iter_list("/developer/v1/users"))

    assert len(session.calls) == 1


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"page": {"next": None}}, "data"),
        ({"data": {}, "page": {"next": None}}, "data"),
        ({"data": []}, "page"),
        ({"data": [], "page": []}, "page"),
        ({"data": [], "page": {"next": 7}}, "page.next"),
        ({"data": ["not-an-object"], "page": {"next": None}}, "non-object"),
    ],
)
def test_iter_list_rejects_malformed_envelopes(payload: Any, message: str) -> None:
    """Malformed page envelopes fail rather than silently truncating data."""
    client, _ = make_client([response(200, payload)])

    with pytest.raises(RampApiError, match=message):
        list(client.iter_list("/developer/v1/users"))


def test_iter_list_rejects_repeated_continuation_url() -> None:
    """A repeated continuation cannot create an infinite pagination loop."""
    next_url = "https://demo-api.ramp.com/developer/v1/users?start=repeat"
    client, session = make_client(
        [
            response(200, {"data": [], "page": {"next": next_url}}),
            response(
                200,
                {"data": [], "page": {"next": next_url}},
                url=next_url,
            ),
        ]
    )

    with pytest.raises(RampApiError, match="repeated"):
        list(client.iter_list("/developer/v1/users"))

    assert len(session.calls) == 2


def test_business_id_is_validated_and_cached() -> None:
    """Tenant resolution makes one authenticated lookup per client instance."""
    client, session = make_client([response(200, {"id": "00000000-0000-4000-8000-000000000001"})])

    assert client.get_business_id() == "00000000-0000-4000-8000-000000000001"
    assert client.get_business_id() == "00000000-0000-4000-8000-000000000001"
    assert len(session.calls) == 1


@pytest.mark.parametrize("payload", [{}, {"id": None}, {"id": 123}])
def test_business_id_must_be_a_nonempty_string(payload: Any) -> None:
    """A malformed identity response cannot weaken tenant isolation."""
    client, _ = make_client([response(200, payload)])

    with pytest.raises(RampApiError, match="business id"):
        client.get_business_id()
