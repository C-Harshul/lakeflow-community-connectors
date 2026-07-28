"""Tests for the QuickBooks pre-pipeline token refresh task."""

from unittest.mock import Mock

import pytest

from databricks.labs.community_connector.sources.quickbooks.quickbooks_token_refresh import (
    TOKEN_ENDPOINT,
    build_connection_options,
    exchange_refresh_token,
    tenant_binding_comment,
    validate_tenant_binding,
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
    assert "incremental_overlap_seconds" in options["externalOptionsAllowList"]
    assert "max_incremental_window_seconds" in options["externalOptionsAllowList"]
    assert "delete_overlap_seconds" in options["externalOptionsAllowList"]
    assert "initial_delete_lookback_seconds" in options["externalOptionsAllowList"]


def test_tenant_binding_accepts_one_consistent_realm() -> None:
    validate_tenant_binding(
        expected_realm_id="realm-a",
        secret_realm_id="realm-a",
        connection_comment=tenant_binding_comment("realm-a"),
    )


@pytest.mark.parametrize(
    "expected,secret,connection,match",
    [
        ("", "realm-a", "realm-a", "expected_realm_id"),
        ("realm-a", "realm-b", "realm-a", "secret scope"),
        ("realm-a", "realm-a", "realm-b", "Unity Catalog connection"),
    ],
)
def test_tenant_binding_rejects_cross_tenant_refresh(
    expected: str,
    secret: str,
    connection: str,
    match: str,
) -> None:
    with pytest.raises((ValueError, RuntimeError), match=match):
        validate_tenant_binding(
            expected_realm_id=expected,
            secret_realm_id=secret,
            connection_comment=tenant_binding_comment(connection),
        )


def test_rejected_tenant_binding_does_not_affect_another_tenant() -> None:
    with pytest.raises(RuntimeError, match="secret scope"):
        validate_tenant_binding(
            expected_realm_id="realm-a",
            secret_realm_id="revoked-or-wrong-realm",
            connection_comment=tenant_binding_comment("realm-a"),
        )

    validate_tenant_binding(
        expected_realm_id="realm-b",
        secret_realm_id="realm-b",
        connection_comment=tenant_binding_comment("realm-b"),
    )
