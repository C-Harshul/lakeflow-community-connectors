"""Tests for Databricks CLI profile discovery and browser login."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from databricks.labs.community_connector_cli.databricks_auth import (
    DatabricksCliProfile,
    list_databricks_profiles,
    login_databricks_profile,
    normalize_workspace_url,
)


def test_list_profiles_returns_non_secret_workspace_metadata():
    run = MagicMock(
        return_value=SimpleNamespace(
            returncode=0,
            stdout=(
                '{"profiles": ['
                '{"name": "fresh", "host": "https://dbc.example.com",'
                '"workspace_id": "123", "token": "must-not-be-read"},'
                '{"name": "account-only"}'
                "]}"
            ),
        )
    )

    profiles = list_databricks_profiles(run=run)

    assert profiles == [
        DatabricksCliProfile(
            name="fresh",
            host="https://dbc.example.com",
            workspace_id="123",
        )
    ]
    assert run.call_args.args[0] == [
        "databricks",
        "auth",
        "profiles",
        "--output",
        "json",
        "--skip-validate",
    ]


def test_normalize_workspace_url_accepts_browser_url_and_removes_ui_fields():
    result = normalize_workspace_url(
        "dbc.example.com/?autoLogin=true&o=123&account_id=abc&ignored=value"
    )

    assert result == "https://dbc.example.com?o=123&account_id=abc"


@pytest.mark.parametrize(
    "value",
    [
        "http://dbc.example.com",
        "https://",
        "https://user:password@dbc.example.com",
    ],
)
def test_normalize_workspace_url_rejects_unsafe_values(value):
    with pytest.raises(ValueError, match="workspace URL"):
        normalize_workspace_url(value)


def test_login_profile_runs_browser_auth_without_shell_interpolation():
    run = MagicMock(return_value=SimpleNamespace(returncode=0))

    login_databricks_profile(
        profile_name="fresh workspace",
        workspace_url="https://dbc.example.com/?o=123",
        run=run,
    )

    assert run.call_args.args[0] == [
        "databricks",
        "auth",
        "login",
        "--host",
        "https://dbc.example.com?o=123",
        "--profile",
        "fresh workspace",
    ]
    assert run.call_args.kwargs == {"check": False}


def test_login_profile_reports_cli_failure_without_echoing_output():
    run = MagicMock(
        return_value=SimpleNamespace(
            returncode=1,
            stderr="sensitive authentication output",
        )
    )

    with pytest.raises(RuntimeError, match="exit code 1") as exc_info:
        login_databricks_profile(
            profile_name="fresh",
            workspace_url="https://dbc.example.com",
            run=run,
        )

    assert "sensitive" not in str(exc_info.value)
