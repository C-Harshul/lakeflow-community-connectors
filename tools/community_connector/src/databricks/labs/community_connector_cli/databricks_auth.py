"""Databricks CLI profile discovery and interactive-login helpers."""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import Callable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


@dataclass(frozen=True)
class DatabricksCliProfile:
    """Non-secret metadata for one local Databricks CLI profile."""

    name: str
    host: str
    workspace_id: str | None = None


def list_databricks_profiles(
    *,
    run: Callable = subprocess.run,
) -> list[DatabricksCliProfile]:
    """Read configured profile names and workspace URLs without refreshing tokens."""
    try:
        result = run(
            [
                "databricks",
                "auth",
                "profiles",
                "--output",
                "json",
                "--skip-validate",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "Databricks CLI was not found; install it before running setup"
        ) from exc
    if result.returncode != 0:
        raise RuntimeError(
            f"Could not list Databricks CLI profiles (exit code {result.returncode})"
        )
    try:
        payload = json.loads(result.stdout)
    except (TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError("Databricks CLI returned invalid profile JSON") from exc
    raw_profiles = payload.get("profiles") if isinstance(payload, dict) else None
    if not isinstance(raw_profiles, list):
        raise RuntimeError("Databricks CLI profile response omitted profiles")

    profiles = []
    for item in raw_profiles:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        host = item.get("host")
        if not isinstance(name, str) or not name.strip():
            continue
        if not isinstance(host, str) or not host.strip():
            continue
        workspace_id = item.get("workspace_id")
        profiles.append(
            DatabricksCliProfile(
                name=name.strip(),
                host=normalize_workspace_url(host),
                workspace_id=(
                    str(workspace_id).strip()
                    if workspace_id is not None and str(workspace_id).strip()
                    else None
                ),
            )
        )
    return profiles


def normalize_workspace_url(value: str) -> str:
    """Validate a workspace URL and retain only login-relevant query fields."""
    raw = value.strip()
    if "://" not in raw:
        raw = f"https://{raw}"
    parsed = urlsplit(raw)
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError("workspace URL must be a valid HTTPS URL")
    if parsed.username or parsed.password:
        raise ValueError("workspace URL must not contain credentials")

    allowed_query = [
        (key, item)
        for key, item in parse_qsl(parsed.query, keep_blank_values=False)
        if key in {"o", "workspace_id", "account_id"}
    ]
    return urlunsplit(
        (
            "https",
            parsed.netloc,
            "",
            urlencode(allowed_query),
            "",
        )
    )


def login_databricks_profile(
    *,
    profile_name: str,
    workspace_url: str,
    run: Callable = subprocess.run,
) -> None:
    """Run browser OAuth and persist it under a user-selected profile name."""
    name = profile_name.strip()
    if not name:
        raise ValueError("profile name cannot be empty")
    host = normalize_workspace_url(workspace_url)
    try:
        result = run(
            [
                "databricks",
                "auth",
                "login",
                "--host",
                host,
                "--profile",
                name,
            ],
            check=False,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "Databricks CLI was not found; install it before running setup"
        ) from exc
    if result.returncode != 0:
        raise RuntimeError(
            f"Databricks browser login failed with exit code {result.returncode}"
        )
