"""Tests for the interactive QuickBooks workspace setup workflow."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner
from databricks.labs.community_connector_cli.cli import (
    _deploy_or_update_quickbooks_pipeline,
    create_pipeline,
    main,
)
from databricks.labs.community_connector_cli.databricks_auth import (
    DatabricksCliProfile,
)
from databricks.labs.community_connector_cli.quickbooks_setup import (
    QUICKBOOKS_TABLES,
    QuickBooksOAuthTokens,
    QuickBooksResourceNames,
    QuickBooksSetupPlan,
    QuickBooksWorkspaceProvisioner,
    authorize_quickbooks,
    build_job_settings,
    build_pipeline_spec,
    default_resource_names,
    exchange_authorization_code,
    slugify_tenant_key,
    tenant_binding_comment,
    validate_quickbooks_company_access,
)
from databricks.sdk.errors import NotFound


def _plan() -> QuickBooksSetupPlan:
    return QuickBooksSetupPlan(
        names=QuickBooksResourceNames(
            tenant_key="acme",
            catalog="finance",
            schema="quickbooks_acme_sandbox",
            secret_scope="quickbooks_acme_sandbox_secrets",
            connection_name="quickbooks_acme_sandbox",
            pipeline_name="quickbooks_acme_sandbox",
            job_name="quickbooks_acme_sandbox_with_token_refresh",
            workspace_path="/Users/user@example.com/connectors/quickbooks_acme_sandbox",
        ),
        environment="sandbox",
        minor_version="75",
    )


def test_defaults_are_derived_from_tenant_and_environment():
    names = default_resource_names(
        tenant_key="Acme & Sons",
        environment="production",
        current_user="owner@example.com",
        catalog="finance",
    )

    assert names.tenant_key == "acme_sons"
    assert names.catalog == "finance"
    assert names.schema == "quickbooks_acme_sons_production"
    assert names.connection_name == "quickbooks_acme_sons_production"
    assert names.workspace_path.startswith("/Users/owner@example.com/")


def test_slug_prefixes_numeric_tenant_and_rejects_empty_value():
    assert slugify_tenant_key("123 North") == "tenant_123_north"
    with pytest.raises(ValueError, match="letter or number"):
        slugify_tenant_key("---")


def test_pipeline_spec_uses_selected_connection_and_tenant_safe_keys():
    spec = build_pipeline_spec("chosen_connection")
    tables = {
        item["table"]["source_table"]: item["table"]["table_configuration"]
        for item in spec["objects"]
    }

    assert spec["connection_name"] == "chosen_connection"
    assert tuple(tables) == QUICKBOOKS_TABLES
    assert all(config["primary_keys"] == ["realm_id", "id"] for config in tables.values())
    assert "delete_overlap_seconds" not in tables["customers"]
    assert tables["invoices"]["delete_overlap_seconds"] == "60"
    assert tables["bills"]["initial_delete_lookback_seconds"] == "300"


def test_tenant_binding_is_deterministic_and_does_not_expose_realm():
    first = tenant_binding_comment("realm-123")
    second = tenant_binding_comment("realm-123")

    assert first == second
    assert first.startswith("quickbooks-realm-sha256:")
    assert "realm-123" not in first


def test_exchange_authorization_code_uses_basic_auth_and_returns_tokens():
    response = MagicMock(status_code=200)
    response.json.return_value = {
        "access_token": "access",
        "refresh_token": "refresh",
    }
    post = MagicMock(return_value=response)

    result = exchange_authorization_code(
        client_id="client",
        client_secret="secret",
        authorization_code="code",
        redirect_uri="http://localhost:8765/oauth/callback",
        post=post,
    )

    assert result == ("access", "refresh")
    assert post.call_args.kwargs["auth"] == ("client", "secret")
    assert post.call_args.kwargs["data"]["grant_type"] == "authorization_code"


def test_exchange_authorization_code_failure_does_not_echo_provider_payload():
    response = MagicMock(status_code=401, text="sensitive provider response")

    with pytest.raises(RuntimeError, match="HTTP 401") as exc_info:
        exchange_authorization_code(
            client_id="client",
            client_secret="secret",
            authorization_code="code",
            redirect_uri="http://localhost:8765/oauth/callback",
            post=MagicMock(return_value=response),
        )

    assert "sensitive" not in str(exc_info.value)


def test_authorize_quickbooks_captures_realm_from_callback():
    def fake_flow(**kwargs):
        kwargs["callback_params_out"]["realmID"] = "realm-123"
        return "code", "", "http://localhost:8765/oauth/callback"

    response = MagicMock(status_code=200)
    response.json.return_value = {
        "access_token": "access",
        "refresh_token": "refresh",
    }

    with patch(
        "databricks.labs.community_connector_cli.quickbooks_setup."
        "run_u2m_authorization_code_flow",
        side_effect=fake_flow,
    ):
        tokens = authorize_quickbooks(
            client_id="client",
            client_secret="secret",
            redirect_port=8765,
            post=MagicMock(return_value=response),
        )

    assert tokens == QuickBooksOAuthTokens("access", "refresh", "realm-123")


def test_authorize_quickbooks_explains_callback_without_company():
    def fake_flow(**kwargs):
        kwargs["callback_params_out"]["unexpected"] = "sensitive-value"
        return "code", "", "http://localhost:8765/oauth/callback"

    with (
        patch(
            "databricks.labs.community_connector_cli.quickbooks_setup."
            "run_u2m_authorization_code_flow",
            side_effect=fake_flow,
        ),
        pytest.raises(RuntimeError, match="did not include realmId") as exc_info,
    ):
        authorize_quickbooks(
            client_id="client",
            client_secret="secret",
            redirect_port=8765,
        )

    assert "unexpected" in str(exc_info.value)
    assert "sensitive-value" not in str(exc_info.value)


def test_authorize_quickbooks_resolves_missing_realm_interactively():
    def fake_flow(**_kwargs):
        return "code", "", "http://localhost:8765/oauth/callback"

    response = MagicMock(status_code=200)
    response.json.return_value = {
        "access_token": "access",
        "refresh_token": "refresh",
    }

    with patch(
        "databricks.labs.community_connector_cli.quickbooks_setup."
        "run_u2m_authorization_code_flow",
        side_effect=fake_flow,
    ):
        tokens = authorize_quickbooks(
            client_id="client",
            client_secret="secret",
            redirect_port=8765,
            realm_id_resolver=lambda: " 123456789 ",
            post=MagicMock(return_value=response),
        )

    assert tokens.realm_id == "123456789"


def test_company_access_validation_proves_token_realm_pair():
    response = MagicMock(status_code=200)
    # CompanyInfo.Id is an entity identifier and need not equal the realm ID.
    response.json.return_value = {"CompanyInfo": {"Id": "1", "CompanyName": "Sandbox"}}
    get = MagicMock(return_value=response)

    validate_quickbooks_company_access(
        tokens=QuickBooksOAuthTokens("access", "refresh", "123456789"),
        environment="sandbox",
        minor_version="75",
        get=get,
    )

    assert get.call_args.args[0] == (
        "https://sandbox-quickbooks.api.intuit.com/v3/company/"
        "123456789/companyinfo/123456789"
    )
    assert get.call_args.kwargs["params"] == {"minorversion": "75"}
    assert get.call_args.kwargs["headers"]["Authorization"] == "Bearer access"


def test_company_access_validation_requires_company_info_payload():
    response = MagicMock(status_code=200)
    response.json.return_value = {"time": "2026-07-29T00:00:00Z"}

    with pytest.raises(RuntimeError, match="omitted CompanyInfo"):
        validate_quickbooks_company_access(
            tokens=QuickBooksOAuthTokens("access", "refresh", "123456789"),
            environment="sandbox",
            minor_version="75",
            get=MagicMock(return_value=response),
        )


def test_company_access_validation_rejects_wrong_realm_without_payload_leak():
    response = MagicMock(status_code=401, text="sensitive QuickBooks response")

    with pytest.raises(RuntimeError, match="HTTP 401") as exc_info:
        validate_quickbooks_company_access(
            tokens=QuickBooksOAuthTokens("access", "refresh", "123456789"),
            environment="sandbox",
            minor_version="75",
            get=MagicMock(return_value=response),
        )

    assert "sensitive" not in str(exc_info.value)


def test_job_runs_refresh_before_the_selected_pipeline():
    settings = build_job_settings(
        plan=_plan(),
        pipeline_id="pipeline-123",
        refresh_notebook_path="/Workspace/refresh",
        realm_id="realm-123",
    )

    refresh, ingestion = settings["tasks"]
    assert settings["name"] == _plan().names.job_name
    assert refresh["notebook_task"]["base_parameters"]["secret_scope"] == (
        _plan().names.secret_scope
    )
    assert refresh["notebook_task"]["base_parameters"]["expected_realm_id"] == "realm-123"
    assert ingestion["depends_on"] == [{"task_key": "refresh_quickbooks_token"}]
    assert ingestion["pipeline_task"]["pipeline_id"] == "pipeline-123"


def test_secret_scope_is_created_and_all_required_values_are_written():
    workspace = MagicMock()
    workspace.secrets.list_scopes.return_value = []
    provisioner = QuickBooksWorkspaceProvisioner(workspace)

    action = provisioner.ensure_secret_scope(
        scope="tenant_scope",
        client_id="client",
        client_secret="secret",
        tokens=QuickBooksOAuthTokens("access", "refresh", "realm"),
    )

    assert action == "created"
    workspace.secrets.create_scope.assert_called_once_with(scope="tenant_scope")
    written_keys = {
        call.kwargs["key"] for call in workspace.secrets.put_secret.call_args_list
    }
    assert written_keys == {"client_id", "client_secret", "refresh_token", "realm_id"}
    assert all(
        call.kwargs["string_value"] != "access"
        for call in workspace.secrets.put_secret.call_args_list
    )


def test_existing_connection_requires_the_same_realm_binding():
    workspace = MagicMock()
    workspace.api_client.do.return_value = {
        "comment": tenant_binding_comment("other-realm")
    }
    provisioner = QuickBooksWorkspaceProvisioner(workspace)

    with pytest.raises(RuntimeError, match="different or missing"):
        provisioner.ensure_connection(
            plan=_plan(),
            access_token="access",
            realm_id="realm-123",
        )

    assert workspace.api_client.do.call_count == 1


def test_missing_connection_is_created_with_dynamic_name():
    workspace = MagicMock()
    workspace.api_client.do.side_effect = [NotFound("missing"), {}]
    provisioner = QuickBooksWorkspaceProvisioner(workspace)

    action = provisioner.ensure_connection(
        plan=_plan(),
        access_token="access",
        realm_id="realm-123",
    )

    assert action == "created"
    create_call = workspace.api_client.do.call_args_list[1]
    assert create_call.args[:2] == (
        "POST",
        "/api/2.1/unity-catalog/connections",
    )
    assert create_call.kwargs["body"]["name"] == _plan().names.connection_name


def test_existing_job_is_reset_instead_of_duplicated():
    workspace = MagicMock()
    existing = SimpleNamespace(
        job_id=42,
        settings=SimpleNamespace(name=_plan().names.job_name),
    )
    workspace.jobs.list.return_value = [existing]
    provisioner = QuickBooksWorkspaceProvisioner(workspace)
    settings = {"name": _plan().names.job_name, "tasks": []}

    action, job_id = provisioner.ensure_job(settings=settings)

    assert (action, job_id) == ("updated", 42)
    workspace.api_client.do.assert_called_once_with(
        "POST",
        "/api/2.2/jobs/reset",
        body={"job_id": 42, "new_settings": settings},
    )


def test_cli_dry_run_prompts_for_no_credentials_or_workspace_mutations():
    workspace = MagicMock()
    workspace.current_user.me.return_value = SimpleNamespace(
        user_name="owner@example.com"
    )

    with patch(
        "databricks.labs.community_connector_cli.cli._make_workspace_client",
        return_value=workspace,
    ):
        result = CliRunner().invoke(
            main,
            [
                "setup_quickbooks",
                "--profile",
                "target-workspace",
                "--tenant",
                "Acme",
                "--environment",
                "sandbox",
                "--catalog",
                "finance",
                "--schema",
                "qb_data",
                "--secret-scope",
                "qb_secrets",
                "--connection-name",
                "qb_connection",
                "--pipeline-name",
                "qb_pipeline",
                "--job-name",
                "qb_job",
                "--workspace-path",
                "/Users/owner@example.com/qb",
                "--dry-run",
            ],
        )

    assert result.exit_code == 0, result.output
    assert "No Intuit OAuth flow or Databricks workspace mutation" in result.output
    assert "qb_connection" in result.output
    workspace.secrets.create_scope.assert_not_called()


def test_cli_lists_existing_profile_with_complete_workspace_url():
    workspace = MagicMock()
    workspace.current_user.me.return_value = SimpleNamespace(
        user_name="owner@example.com"
    )
    profiles = [
        DatabricksCliProfile(
            name="fresh-workspace",
            host="https://dbc.example.com",
            workspace_id="123456",
        )
    ]

    with (
        patch(
            "databricks.labs.community_connector_cli.databricks_auth."
            "list_databricks_profiles",
            return_value=profiles,
        ),
        patch(
            "databricks.labs.community_connector_cli.cli._make_workspace_client",
            return_value=workspace,
        ) as make_client,
    ):
        result = CliRunner().invoke(
            main,
            [
                "setup_quickbooks",
                "--tenant",
                "Acme",
                "--environment",
                "sandbox",
                "--catalog",
                "workspace",
                "--schema",
                "qb_data",
                "--secret-scope",
                "qb_secrets",
                "--connection-name",
                "qb_connection",
                "--pipeline-name",
                "qb_pipeline",
                "--job-name",
                "qb_job",
                "--workspace-path",
                "/Users/owner@example.com/qb",
                "--dry-run",
            ],
            input="1\n",
        )

    assert result.exit_code == 0, result.output
    assert "fresh-workspace" in result.output
    assert "https://dbc.example.com/?o=123456" in result.output
    make_client.assert_called_once_with("fresh-workspace")


def test_cli_can_create_profile_before_dry_run():
    workspace = MagicMock()
    workspace.current_user.me.return_value = SimpleNamespace(
        user_name="owner@example.com"
    )

    with (
        patch(
            "databricks.labs.community_connector_cli.databricks_auth."
            "list_databricks_profiles",
            return_value=[],
        ),
        patch(
            "databricks.labs.community_connector_cli.databricks_auth."
            "login_databricks_profile"
        ) as login,
        patch(
            "databricks.labs.community_connector_cli.cli._make_workspace_client",
            return_value=workspace,
        ) as make_client,
    ):
        result = CliRunner().invoke(
            main,
            [
                "setup_quickbooks",
                "--tenant",
                "Acme",
                "--environment",
                "sandbox",
                "--catalog",
                "workspace",
                "--schema",
                "qb_data",
                "--secret-scope",
                "qb_secrets",
                "--connection-name",
                "qb_connection",
                "--pipeline-name",
                "qb_pipeline",
                "--job-name",
                "qb_job",
                "--workspace-path",
                "/Users/owner@example.com/qb",
                "--dry-run",
            ],
            input=(
                "1\n"
                "new-fresh-profile\n"
                "https://dbc.example.com/?autoLogin=true&o=123456\n"
            ),
        )

    assert result.exit_code == 0, result.output
    login.assert_called_once_with(
        profile_name="new-fresh-profile",
        workspace_url="https://dbc.example.com?o=123456",
    )
    make_client.assert_called_once_with("new-fresh-profile")
    assert "Databricks profile created: new-fresh-profile" in result.output


def test_cli_can_reauthenticate_selected_existing_profile():
    workspace = MagicMock()
    workspace.current_user.me.return_value = SimpleNamespace(
        user_name="owner@example.com"
    )
    selected = DatabricksCliProfile(
        name="expired-profile",
        host="https://dbc.example.com",
        workspace_id="123456",
    )

    with (
        patch(
            "databricks.labs.community_connector_cli.databricks_auth."
            "list_databricks_profiles",
            return_value=[selected],
        ),
        patch(
            "databricks.labs.community_connector_cli.databricks_auth."
            "login_databricks_profile"
        ) as login,
        patch(
            "databricks.labs.community_connector_cli.cli._make_workspace_client",
            side_effect=[ValueError("expired"), workspace],
        ),
    ):
        result = CliRunner().invoke(
            main,
            [
                "setup_quickbooks",
                "--tenant",
                "Acme",
                "--environment",
                "sandbox",
                "--catalog",
                "workspace",
                "--schema",
                "qb_data",
                "--secret-scope",
                "qb_secrets",
                "--connection-name",
                "qb_connection",
                "--pipeline-name",
                "qb_pipeline",
                "--job-name",
                "qb_job",
                "--workspace-path",
                "/Users/owner@example.com/qb",
                "--dry-run",
            ],
            input="1\ny\n",
        )

    assert result.exit_code == 0, result.output
    login.assert_called_once_with(
        profile_name="expired-profile",
        workspace_url="https://dbc.example.com",
    )


def test_cli_reports_expired_databricks_profile_without_a_traceback():
    with patch(
        "databricks.labs.community_connector_cli.cli._make_workspace_client",
        side_effect=ValueError("sensitive SDK authentication details"),
    ):
        result = CliRunner().invoke(
            main,
            ["setup_quickbooks", "--profile", "expired-profile"],
        )

    assert result.exit_code != 0
    assert "Could not authenticate with 'expired-profile'" in result.output
    assert "databricks auth login --profile expired-profile" in result.output
    assert "sensitive SDK" not in result.output
    assert "Traceback" not in result.output


def test_pipeline_deployment_reuses_setup_workspace_client():
    context = MagicMock()
    workspace = MagicMock()

    with patch(
        "databricks.labs.community_connector_cli.cli._find_exact_pipeline_by_name",
        side_effect=[None, "pipeline-123"],
    ):
        pipeline_id = _deploy_or_update_quickbooks_pipeline(
            context,
            workspace_client=workspace,
            plan=_plan(),
            pipeline_spec_path="/tmp/spec.yaml",
            deployment_config_path="/tmp/deployment.yaml",
            repo_url=None,
        )

    assert pipeline_id == "pipeline-123"
    assert context.invoke.call_args.args[0] is create_pipeline
    assert context.invoke.call_args.kwargs["_workspace_client"] is workspace
