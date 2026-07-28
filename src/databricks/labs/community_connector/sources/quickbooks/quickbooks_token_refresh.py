"""Databricks workflow task for refreshing a QuickBooks connection.

This module is deployed as a serverless notebook task that runs immediately
before the ingestion pipeline. Secrets stay in a Databricks secret scope; the
Unity Catalog connection receives only the short-lived access token.
"""

from __future__ import annotations

import hashlib
from typing import Callable
from urllib.parse import quote

import requests

TOKEN_ENDPOINT = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
DEFAULT_TIMEOUT_SECONDS = 30
TENANT_BINDING_PREFIX = "quickbooks-realm-sha256:"


def tenant_binding_comment(realm_id: str) -> str:
    """Return a non-reversible connection marker for one QuickBooks realm."""
    digest = hashlib.sha256(realm_id.encode("utf-8")).hexdigest()
    return f"{TENANT_BINDING_PREFIX}{digest}"


def validate_tenant_binding(
    *,
    expected_realm_id: str,
    secret_realm_id: str,
    connection_comment: str,
) -> None:
    """Fail before token rotation if a Job, secret scope, and connection differ."""
    if not expected_realm_id:
        raise ValueError("expected_realm_id is required for tenant-isolated refresh")
    if secret_realm_id != expected_realm_id:
        raise RuntimeError(
            "QuickBooks tenant binding failed: the secret scope realm_id does not "
            "match this Job's expected_realm_id"
        )
    if connection_comment != tenant_binding_comment(expected_realm_id):
        raise RuntimeError(
            "QuickBooks tenant binding failed: the Unity Catalog connection binding "
            "does not match this Job's expected_realm_id"
        )


def exchange_refresh_token(
    client_id: str,
    client_secret: str,
    refresh_token: str,
    *,
    post: Callable = requests.post,
) -> tuple[str, str]:
    """Exchange and rotate an Intuit refresh token using HTTP Basic auth."""
    response = post(
        TOKEN_ENDPOINT,
        auth=(client_id, client_secret),
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        headers={"Accept": "application/json"},
        timeout=DEFAULT_TIMEOUT_SECONDS,
    )
    if response.status_code != 200:
        raise RuntimeError(f"QuickBooks token refresh failed with HTTP {response.status_code}")
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("QuickBooks token refresh returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("QuickBooks token refresh returned an invalid JSON object")
    access_token = payload.get("access_token")
    next_refresh_token = payload.get("refresh_token")
    if not access_token or not next_refresh_token:
        raise RuntimeError("QuickBooks token refresh omitted access_token or refresh_token")
    return str(access_token), str(next_refresh_token)


def build_connection_options(
    *,
    access_token: str,
    realm_id: str,
    environment: str,
    minor_version: str,
) -> dict[str, str]:
    """Build the complete static COMMUNITY connection option set."""
    return {
        "access_token": access_token,
        "realm_id": realm_id,
        "environment": environment,
        "minor_version": minor_version,
        "externalOptionsAllowList": (
            "delete_overlap_seconds,incremental_overlap_seconds,"
            "initial_delete_lookback_seconds,isDeleteFlow,"
            "max_incremental_window_seconds,page_size,"
            "tableConfigs,tableName,tableNameList"
        ),
        "sourceName": "quickbooks",
    }


def run_refresh_task(dbutils) -> None:
    """Refresh secrets and update the UC connection for the next pipeline run."""
    from databricks.sdk import WorkspaceClient

    secret_scope = dbutils.widgets.get("secret_scope")
    connection_name = dbutils.widgets.get("connection_name")
    expected_realm_id = dbutils.widgets.get("expected_realm_id").strip()
    environment = dbutils.widgets.get("environment")
    minor_version = dbutils.widgets.get("minor_version")

    realm_id = dbutils.secrets.get(
        scope=secret_scope,
        key="realm_id",
    )

    workspace = WorkspaceClient()
    connection = workspace.api_client.do(
        "GET",
        (f"/api/2.1/unity-catalog/connections/{quote(connection_name, safe='')}"),
    )
    if not isinstance(connection, dict) or not isinstance(connection.get("comment"), str):
        raise RuntimeError("Unity Catalog connection is missing its QuickBooks tenant binding")
    validate_tenant_binding(
        expected_realm_id=expected_realm_id,
        secret_realm_id=realm_id,
        connection_comment=connection["comment"],
    )

    client_id = dbutils.secrets.get(
        scope=secret_scope,
        key="client_id",
    )
    client_secret = dbutils.secrets.get(
        scope=secret_scope,
        key="client_secret",
    )
    refresh_token = dbutils.secrets.get(
        scope=secret_scope,
        key="refresh_token",
    )
    access_token, next_refresh_token = exchange_refresh_token(
        client_id,
        client_secret,
        refresh_token,
    )

    # Persist Intuit's rotated token before updating the connection. If the
    # connection update fails, the next task run can still retry safely.
    workspace.secrets.put_secret(
        secret_scope,
        "refresh_token",
        string_value=next_refresh_token,
    )
    options = build_connection_options(
        access_token=access_token,
        realm_id=realm_id,
        environment=environment,
        minor_version=minor_version,
    )
    workspace.api_client.do(
        "PATCH",
        (f"/api/2.1/unity-catalog/connections/{quote(connection_name, safe='')}"),
        body={
            "name": connection_name,
            "comment": connection["comment"],
            "options": options,
        },
    )
    print("QuickBooks access token refreshed and connection updated.")


if __name__ == "__main__":
    run_refresh_task(dbutils)  # type: ignore[name-defined]  # noqa: F821
