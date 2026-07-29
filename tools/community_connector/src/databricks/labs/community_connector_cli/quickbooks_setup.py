"""Modular provisioning helpers for the QuickBooks connector setup command."""

from __future__ import annotations

import base64
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from urllib.parse import quote

import requests

from databricks.labs.community_connector_cli.oauth_flow import (
    run_u2m_authorization_code_flow,
)
from databricks.sdk.errors import NotFound, ResourceDoesNotExist
from databricks.sdk.service.workspace import ImportFormat, Language

QUICKBOOKS_AUTHORIZATION_ENDPOINT = "https://appcenter.intuit.com/connect/oauth2"
QUICKBOOKS_TOKEN_ENDPOINT = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
QUICKBOOKS_SCOPE = "com.intuit.quickbooks.accounting"
QUICKBOOKS_API_BASE_URLS = {
    "sandbox": "https://sandbox-quickbooks.api.intuit.com",
    "production": "https://quickbooks.api.intuit.com",
}
TENANT_BINDING_PREFIX = "quickbooks-realm-sha256:"
DEFAULT_MINOR_VERSION = "75"
DEFAULT_REDIRECT_PORT = 8765
DEFAULT_TIMEOUT_SECONDS = 30
QUICKBOOKS_TABLES = (
    "customers",
    "vendors",
    "accounts",
    "items",
    "invoices",
    "bills",
)
EXTERNAL_OPTIONS_ALLOWLIST = (
    "delete_overlap_seconds,incremental_overlap_seconds,"
    "initial_delete_lookback_seconds,isDeleteFlow,"
    "max_incremental_window_seconds,page_size,"
    "tableConfigs,tableName,tableNameList"
)


@dataclass(frozen=True)
class QuickBooksResourceNames:
    """User-selected names for one tenant's Databricks resources."""

    tenant_key: str
    catalog: str
    schema: str
    secret_scope: str
    connection_name: str
    pipeline_name: str
    job_name: str
    workspace_path: str


@dataclass(frozen=True)
class QuickBooksOAuthTokens:
    """OAuth values returned by Intuit after user consent."""

    access_token: str
    refresh_token: str
    realm_id: str


@dataclass(frozen=True)
class QuickBooksSetupPlan:
    """Complete non-secret deployment plan for one QuickBooks realm."""

    names: QuickBooksResourceNames
    environment: str
    minor_version: str = DEFAULT_MINOR_VERSION


def slugify_tenant_key(value: str) -> str:
    """Convert a display name into a stable resource-name component."""
    slug = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
    if not slug:
        raise ValueError("tenant name must contain at least one letter or number")
    if slug[0].isdigit():
        slug = f"tenant_{slug}"
    return slug


def default_resource_names(
    *,
    tenant_key: str,
    environment: str,
    current_user: str,
    catalog: str = "workspace",
) -> QuickBooksResourceNames:
    """Build deterministic defaults without fixing names in the command."""
    slug = slugify_tenant_key(tenant_key)
    suffix = f"{slug}_{environment}"
    pipeline_name = f"quickbooks_{suffix}"
    return QuickBooksResourceNames(
        tenant_key=slug,
        catalog=catalog,
        schema=f"quickbooks_{suffix}",
        secret_scope=f"quickbooks_{suffix}_secrets",
        connection_name=f"quickbooks_{suffix}",
        pipeline_name=pipeline_name,
        job_name=f"{pipeline_name}_with_token_refresh",
        workspace_path=(
            f"/Users/{current_user}/.lakeflow_community_connectors/{pipeline_name}"
        ),
    )


def validate_setup_plan(plan: QuickBooksSetupPlan) -> None:
    """Validate fields that become API identifiers before any mutation."""
    if plan.environment not in {"sandbox", "production"}:
        raise ValueError("environment must be sandbox or production")
    if not plan.minor_version.isdigit():
        raise ValueError("minor_version must be a positive integer")
    for label, value in (
        ("catalog", plan.names.catalog),
        ("schema", plan.names.schema),
        ("secret scope", plan.names.secret_scope),
        ("connection", plan.names.connection_name),
        ("pipeline", plan.names.pipeline_name),
        ("job", plan.names.job_name),
        ("workspace path", plan.names.workspace_path),
    ):
        if not value.strip():
            raise ValueError(f"{label} name cannot be empty")


def tenant_binding_comment(realm_id: str) -> str:
    """Return the non-reversible connection marker used by the refresh task."""
    digest = hashlib.sha256(realm_id.encode("utf-8")).hexdigest()
    return f"{TENANT_BINDING_PREFIX}{digest}"


def build_connection_options(
    *,
    access_token: str,
    realm_id: str,
    environment: str,
    minor_version: str,
) -> dict[str, str]:
    """Build the static COMMUNITY options consumed by Spark."""
    return {
        "access_token": access_token,
        "realm_id": realm_id,
        "environment": environment,
        "minor_version": minor_version,
        "externalOptionsAllowList": EXTERNAL_OPTIONS_ALLOWLIST,
        "sourceName": "quickbooks",
    }


def build_pipeline_spec(connection_name: str) -> dict:
    """Return the six-table M5 pipeline spec for a chosen connection name."""
    update_config = {
        "scd_type": "SCD_TYPE_1",
        "primary_keys": ["realm_id", "id"],
        "sequence_by": "last_updated_at",
        "incremental_overlap_seconds": "60",
        "max_incremental_window_seconds": "86400",
    }
    transaction_config = {
        **update_config,
        "delete_overlap_seconds": "60",
        "initial_delete_lookback_seconds": "300",
    }
    objects = []
    for table in QUICKBOOKS_TABLES:
        config = transaction_config if table in {"invoices", "bills"} else update_config
        objects.append(
            {
                "table": {
                    "source_table": table,
                    "table_configuration": dict(config),
                }
            }
        )
    return {"connection_name": connection_name, "objects": objects}


def exchange_authorization_code(
    *,
    client_id: str,
    client_secret: str,
    authorization_code: str,
    redirect_uri: str,
    post: Callable = requests.post,
) -> tuple[str, str]:
    """Exchange an Intuit authorization code without logging credentials."""
    response = post(
        QUICKBOOKS_TOKEN_ENDPOINT,
        auth=(client_id, client_secret),
        data={
            "grant_type": "authorization_code",
            "code": authorization_code,
            "redirect_uri": redirect_uri,
        },
        headers={"Accept": "application/json"},
        timeout=DEFAULT_TIMEOUT_SECONDS,
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"QuickBooks authorization-code exchange failed with HTTP {response.status_code}"
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("QuickBooks token exchange returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("QuickBooks token exchange returned an invalid object")
    access_token = payload.get("access_token")
    refresh_token = payload.get("refresh_token")
    if not access_token or not refresh_token:
        raise RuntimeError("QuickBooks token exchange omitted access_token or refresh_token")
    return str(access_token), str(refresh_token)


def authorize_quickbooks(
    *,
    client_id: str,
    client_secret: str,
    redirect_port: int,
    open_browser: bool = True,
    echo: Callable = print,
    post: Callable = requests.post,
    realm_id_resolver: Callable[[], str] | None = None,
) -> QuickBooksOAuthTokens:
    """Run Intuit consent, capture realmId, and exchange the authorization code."""
    callback_params: dict[str, str] = {}
    code, _verifier, redirect_uri = run_u2m_authorization_code_flow(
        client_id=client_id,
        authorization_endpoint=QUICKBOOKS_AUTHORIZATION_ENDPOINT,
        scope=QUICKBOOKS_SCOPE,
        redirect_port=redirect_port,
        redirect_host="localhost",
        redirect_path="/oauth/callback",
        use_pkce=False,
        open_browser=open_browser,
        echo=echo,
        callback_params_out=callback_params,
    )
    realm_id = next(
        (
            value.strip()
            for key, value in callback_params.items()
            if key.casefold().replace("_", "") == "realmid" and value.strip()
        ),
        "",
    )
    if not realm_id:
        if realm_id_resolver is None:
            received_fields = ", ".join(sorted(callback_params)) or "none"
            raise RuntimeError(
                "Intuit OAuth callback did not include realmId. Authorize a QuickBooks "
                "Online company using this app's Development credentials "
                f"(non-protocol callback fields received: {received_fields})"
            )
        realm_id = realm_id_resolver().strip()
        if not realm_id:
            raise RuntimeError("QuickBooks company ID cannot be empty")
    access_token, refresh_token = exchange_authorization_code(
        client_id=client_id,
        client_secret=client_secret,
        authorization_code=code,
        redirect_uri=redirect_uri,
        post=post,
    )
    return QuickBooksOAuthTokens(
        access_token=access_token,
        refresh_token=refresh_token,
        realm_id=realm_id,
    )


def validate_quickbooks_company_access(
    *,
    tokens: QuickBooksOAuthTokens,
    environment: str,
    minor_version: str,
    get: Callable = requests.get,
) -> None:
    """Prove the access token and realm identify the same QuickBooks company."""
    if environment not in QUICKBOOKS_API_BASE_URLS:
        raise RuntimeError("QuickBooks environment must be sandbox or production")
    if not re.fullmatch(r"\d+", tokens.realm_id):
        raise RuntimeError("QuickBooks company ID must contain only digits")

    base_url = QUICKBOOKS_API_BASE_URLS[environment]
    response = get(
        (
            f"{base_url}/v3/company/{tokens.realm_id}"
            f"/companyinfo/{tokens.realm_id}"
        ),
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {tokens.access_token}",
        },
        params={"minorversion": minor_version},
        timeout=DEFAULT_TIMEOUT_SECONDS,
    )
    if response.status_code != 200:
        raise RuntimeError(
            "QuickBooks company verification failed with "
            f"HTTP {response.status_code}; check the company ID and sandbox account"
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("QuickBooks company verification returned invalid JSON") from exc
    company = payload.get("CompanyInfo") if isinstance(payload, dict) else None
    if not isinstance(company, dict) or str(company.get("Id", "")) != tokens.realm_id:
        raise RuntimeError(
            "QuickBooks company verification returned a different or missing company ID"
        )


def build_job_settings(
    *,
    plan: QuickBooksSetupPlan,
    pipeline_id: str,
    refresh_notebook_path: str,
    realm_id: str,
) -> dict:
    """Build an idempotent refresh-then-ingest Job definition."""
    return {
        "name": plan.names.job_name,
        "format": "MULTI_TASK",
        "max_concurrent_runs": 1,
        "queue": {"enabled": True},
        "tags": {
            "source": "quickbooks",
            "tenant": plan.names.tenant_key,
            "managed_by": "community-connector-setup",
        },
        "tasks": [
            {
                "task_key": "refresh_quickbooks_token",
                "notebook_task": {
                    "notebook_path": refresh_notebook_path,
                    "source": "WORKSPACE",
                    "base_parameters": {
                        "secret_scope": plan.names.secret_scope,
                        "connection_name": plan.names.connection_name,
                        "expected_realm_id": realm_id,
                        "environment": plan.environment,
                        "minor_version": plan.minor_version,
                    },
                },
                "max_retries": 2,
                "min_retry_interval_millis": 10000,
            },
            {
                "task_key": "run_quickbooks_pipeline",
                "depends_on": [{"task_key": "refresh_quickbooks_token"}],
                "pipeline_task": {
                    "pipeline_id": pipeline_id,
                    "full_refresh": False,
                },
            },
        ],
    }


class QuickBooksWorkspaceProvisioner:
    """Idempotently provision the non-pipeline Databricks resources."""

    def __init__(self, workspace_client) -> None:
        self._workspace = workspace_client

    def ensure_secret_scope(
        self,
        *,
        scope: str,
        client_id: str,
        client_secret: str,
        tokens: QuickBooksOAuthTokens,
    ) -> str:
        existing = {item.name for item in self._workspace.secrets.list_scopes()}
        action = "updated"
        if scope not in existing:
            self._workspace.secrets.create_scope(scope=scope)
            action = "created"
        for key, value in (
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("refresh_token", tokens.refresh_token),
            ("realm_id", tokens.realm_id),
        ):
            self._workspace.secrets.put_secret(
                scope=scope,
                key=key,
                string_value=value,
            )
        return action

    def ensure_schema(self, *, catalog: str, schema: str) -> str:
        full_name = f"{catalog}.{schema}"
        try:
            self._workspace.schemas.get(full_name=full_name)
            return "reused"
        except (NotFound, ResourceDoesNotExist):
            self._workspace.schemas.create(
                name=schema,
                catalog_name=catalog,
                comment="QuickBooks connector destination managed by setup_quickbooks",
            )
            return "created"

    def ensure_connection(
        self,
        *,
        plan: QuickBooksSetupPlan,
        access_token: str,
        realm_id: str,
    ) -> str:
        path = (
            "/api/2.1/unity-catalog/connections/"
            f"{quote(plan.names.connection_name, safe='')}"
        )
        existing = None
        try:
            existing = self._workspace.api_client.do("GET", path)
        except (NotFound, ResourceDoesNotExist):
            pass

        comment = tenant_binding_comment(realm_id)
        options = build_connection_options(
            access_token=access_token,
            realm_id=realm_id,
            environment=plan.environment,
            minor_version=plan.minor_version,
        )
        if existing is not None:
            if not isinstance(existing, dict) or existing.get("comment") != comment:
                raise RuntimeError(
                    "Existing Unity Catalog connection has a different or missing "
                    "QuickBooks realm binding"
                )
            self._workspace.api_client.do(
                "PATCH",
                path,
                body={
                    "name": plan.names.connection_name,
                    "comment": comment,
                    "options": options,
                },
            )
            return "updated"

        self._workspace.api_client.do(
            "POST",
            "/api/2.1/unity-catalog/connections",
            body={
                "name": plan.names.connection_name,
                "connection_type": "COMMUNITY",
                "comment": comment,
                "options": options,
            },
        )
        return "created"

    def upload_refresh_notebook(
        self,
        *,
        local_source: Path,
        workspace_path: str,
    ) -> None:
        content = base64.b64encode(local_source.read_bytes()).decode("ascii")
        self._workspace.workspace.import_(
            path=workspace_path,
            content=content,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )

    def ensure_job(
        self,
        *,
        settings: dict,
    ) -> tuple[str, int]:
        matches = [
            job
            for job in self._workspace.jobs.list(name=settings["name"])
            if getattr(job, "settings", None)
            and getattr(job.settings, "name", None) == settings["name"]
        ]
        if len(matches) > 1:
            raise RuntimeError(f"Multiple Jobs named {settings['name']!r} exist")
        if matches:
            job_id = int(matches[0].job_id)
            self._workspace.api_client.do(
                "POST",
                "/api/2.2/jobs/reset",
                body={"job_id": job_id, "new_settings": settings},
            )
            return "updated", job_id

        response = self._workspace.api_client.do(
            "POST",
            "/api/2.2/jobs/create",
            body=settings,
        )
        if not isinstance(response, dict) or not isinstance(response.get("job_id"), int):
            raise RuntimeError("Databricks Jobs API did not return a job_id")
        return "created", response["job_id"]

    def run_job(self, job_id: int) -> int:
        response = self._workspace.api_client.do(
            "POST",
            "/api/2.2/jobs/run-now",
            body={"job_id": job_id},
        )
        if not isinstance(response, dict) or not isinstance(response.get("run_id"), int):
            raise RuntimeError("Databricks Jobs API did not return a run_id")
        return response["run_id"]
