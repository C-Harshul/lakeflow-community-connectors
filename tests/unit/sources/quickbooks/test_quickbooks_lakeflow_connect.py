"""Generic connector contract tests for QuickBooks Online."""

from databricks.labs.community_connector.sources.quickbooks.quickbooks import (
    QuickBooksLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestQuickBooksConnector(LakeflowConnectTests):
    connector_class = QuickBooksLakeflowConnect
    simulator_source = "quickbooks"
    replay_config = {
        "access_token": "simulator-token",
        "realm_id": "simulator-realm",
        "environment": "sandbox",
        "minor_version": "75",
    }
    table_configs = {
        table: {"page_size": "1"}
        for table in (
            "customers",
            "vendors",
            "accounts",
            "items",
            "invoices",
            "bills",
        )
    }
