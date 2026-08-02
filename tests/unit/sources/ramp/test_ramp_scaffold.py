"""M2 contract tests for the Ramp connector scaffold."""

import pytest
from pyspark.sql.types import StructType

from databricks.labs.community_connector.sources.ramp import (
    RampDataSource,
    RampLakeflowConnect,
)
from databricks.labs.community_connector.sources.ramp.ramp import (
    API_ROOTS,
    DOWNGRADED_SNAPSHOT_TABLES,
    OFFSET_VERSION,
    TABLE_CONTRACTS,
    TABLE_ENDPOINTS,
    TABLE_SCOPES,
)
from databricks.labs.community_connector.sources.ramp.ramp_schemas import (
    SUPPORTED_TABLES,
)
from databricks.labs.community_connector.sparkpds import find_data_source

REPLAY_CONFIG = {
    "environment": "sandbox",
    "access_token": "simulator-fake-access-token",
}


@pytest.fixture(name="connector")
def connector_fixture() -> RampLakeflowConnect:
    """Build a no-network Ramp connector for static contract tests."""
    return RampLakeflowConnect(dict(REPLAY_CONFIG))


def test_data_source_is_discoverable() -> None:
    """The package must expose the Ramp-bound Spark data source."""
    assert (
        RampDataSource._lakeflow_connect_cls  # pylint: disable=protected-access
        is RampLakeflowConnect
    )
    assert find_data_source("ramp") is RampDataSource


def test_supported_tables_have_complete_static_contracts(
    connector: RampLakeflowConnect,
) -> None:
    """Every selected table must expose stable schema and metadata maps."""
    assert connector.list_tables() == list(SUPPORTED_TABLES)
    assert set(SUPPORTED_TABLES) == set(TABLE_CONTRACTS)
    assert set(SUPPORTED_TABLES) == set(TABLE_ENDPOINTS)
    assert set(SUPPORTED_TABLES) == set(TABLE_SCOPES)

    for table_name in SUPPORTED_TABLES:
        schema = connector.get_table_schema(table_name, {})
        metadata = connector.read_table_metadata(table_name, {})

        assert isinstance(schema, StructType)
        assert schema == connector.get_table_schema(table_name, {})
        assert metadata == connector.read_table_metadata(table_name, {})
        assert metadata["primary_keys"] == ["tenant_id", "id"]
        assert {"tenant_id", "id", "_ramp_extracted_at"}.issubset(schema.fieldNames())

        cursor_field = metadata.get("cursor_field")
        if metadata["ingestion_type"] == "cdc":
            assert cursor_field in schema.fieldNames()
        else:
            assert metadata["ingestion_type"] == "snapshot"
            assert cursor_field is None


def test_no_table_claims_delete_capture(connector: RampLakeflowConnect) -> None:
    """M2 must not advertise a delete flow without a durable Ramp feed."""
    ingestion_types = {
        connector.read_table_metadata(table_name, {})["ingestion_type"]
        for table_name in connector.list_tables()
    }
    assert "cdc_with_deletes" not in ingestion_types


def test_m4_incremental_safety_decision_is_explicit() -> None:
    """Only the endpoint with a provable closed window may advertise CDC."""
    assert DOWNGRADED_SNAPSHOT_TABLES == {
        "transactions",
        "reimbursements",
    }
    assert TABLE_CONTRACTS["transactions"].cursor_filter is None
    assert TABLE_CONTRACTS["transactions"].upper_cursor_filter is None
    assert TABLE_CONTRACTS["reimbursements"].cursor_filter is None
    assert TABLE_CONTRACTS["reimbursements"].upper_cursor_filter is None
    assert TABLE_CONTRACTS["vendors"].cursor_filter == "from_updated_at"
    assert TABLE_CONTRACTS["vendors"].upper_cursor_filter == "to_updated_at"
    assert TABLE_CONTRACTS["vendors"].lower_bound_exclusive is True
    assert TABLE_CONTRACTS["vendors"].upper_bound_inclusive is True


def test_environment_and_access_token_are_validated() -> None:
    """Only known hosts and UC-injected bearer tokens are accepted."""
    connector = RampLakeflowConnect(dict(REPLAY_CONFIG))
    assert connector._api.base_url == API_ROOTS["sandbox"]  # pylint: disable=protected-access
    assert OFFSET_VERSION == 1

    with pytest.raises(ValueError, match="environment"):
        RampLakeflowConnect({"environment": "staging", "access_token": "fake-token"})
    with pytest.raises(ValueError, match="access_token"):
        RampLakeflowConnect({"environment": "sandbox"})


@pytest.mark.parametrize(
    ("table_name", "options", "message"),
    [
        ("users", {"window_seconds": "60"}, "Unsupported options"),
        ("transactions", {"page_size": "1"}, "page_size"),
        ("transactions", {"page_size": "101"}, "page_size"),
        (
            "transactions",
            {"max_records_per_batch": "0"},
            "Unsupported options",
        ),
        ("vendors", {"max_records_per_batch": "1"}, "max_records_per_batch"),
        ("vendors", {"window_seconds": "invalid"}, "window_seconds"),
        (
            "vendors",
            {"start_timestamp": "2026-01-01T00:00:00"},
            "Unsupported options",
        ),
    ],
)
def test_invalid_table_options_are_rejected(
    connector: RampLakeflowConnect,
    table_name: str,
    options: dict[str, str],
    message: str,
) -> None:
    """Unknown and unsafe table-option values fail before any HTTP work."""
    with pytest.raises(ValueError, match=message):
        connector.get_table_schema(table_name, options)


def test_managed_runtime_options_do_not_fail_table_validation(
    connector: RampLakeflowConnect,
) -> None:
    """Databricks-injected connection/runtime fields are not table controls."""
    runtime_options = {
        "access_token": "fake-token",
        "access_token_expiration": "0",
        "client_id": "fake-client-id",
        "client_secret": "fake-client-secret",
        "community_oauth_flow": "m2m",
        "connectionname": "ramp_test_connection",
        "databricks.connection": "ramp_test_connection",
        "dltpipelineid": "fake-pipeline-id",
        "dltupdateid": "fake-update-id",
        "environment": "sandbox",
        "lookback_seconds": "300",
        "max_records_per_batch": "5000",
        "oauth_scope": "business:read",
        "sourcename": "ramp",
        "tablename": "transactions",
        "token_endpoint": "https://demo-api.ramp.com/developer/v1/token",
        "window_seconds": "86400",
        "page_size": "100",
    }

    schema = connector.get_table_schema("transactions", runtime_options)

    assert isinstance(schema, StructType)


def test_invalid_table_is_rejected_before_read(connector: RampLakeflowConnect) -> None:
    """An unknown table fails before any HTTP request is attempted."""
    with pytest.raises(ValueError, match="Unsupported Ramp table"):
        connector.read_table("unknown", {}, {})
