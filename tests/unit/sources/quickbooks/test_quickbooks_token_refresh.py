"""Tests for the QuickBooks pre-pipeline token refresh task."""

from unittest.mock import Mock

import pytest

from databricks.labs.community_connector.sources.quickbooks.quickbooks_token_refresh import (
    TOKEN_ENDPOINT,
    build_connection_options,
    exchange_refresh_token,
)


def test_exchange_refresh_token_uses_basic_auth_and_returns_rotation() -> None:
    response = Mock(status_code=200)
    response.json.return_value = {
        "access_token": "new-access",
        "refresh_token": "new-refresh",
    }
    post = Mock(return_value=response)

    result = exchange_refresh_token(
        "client-id",
        "client-secret",
        "old-refresh",
        post=post,
    )

    assert result == ("new-access", "new-refresh")
    post.assert_called_once_with(
        TOKEN_ENDPOINT,
        auth=("client-id", "client-secret"),
        data={
            "grant_type": "refresh_token",
            "refresh_token": "old-refresh",
        },
        headers={"Accept": "application/json"},
        timeout=30,
    )


def test_exchange_refresh_token_sanitizes_http_failure() -> None:
    response = Mock(status_code=401)
    post = Mock(return_value=response)

    with pytest.raises(RuntimeError, match="HTTP 401"):
        exchange_refresh_token(
            "client-id",
            "client-secret",
            "refresh-token",
            post=post,
        )


def test_exchange_refresh_token_requires_both_rotated_tokens() -> None:
    response = Mock(status_code=200)
    response.json.return_value = {"access_token": "new-access"}
    post = Mock(return_value=response)

    with pytest.raises(RuntimeError, match="omitted"):
        exchange_refresh_token(
            "client-id",
            "client-secret",
            "refresh-token",
            post=post,
        )


def test_build_connection_options_contains_no_long_lived_secrets() -> None:
    options = build_connection_options(
        access_token="short-lived",
        realm_id="realm",
        environment="sandbox",
        minor_version="75",
    )

    assert options["access_token"] == "short-lived"
    assert options["realm_id"] == "realm"
    assert "client_id" not in options
    assert "client_secret" not in options
    assert "refresh_token" not in options
