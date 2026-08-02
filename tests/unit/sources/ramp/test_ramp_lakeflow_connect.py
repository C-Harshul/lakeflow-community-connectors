"""Generic Lakeflow contract suite for Ramp simulate mode."""

from databricks.labs.community_connector.sources.ramp import RampLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestRampConnector(LakeflowConnectTests):
    """Exercise every Ramp table against the local source simulator."""

    connector_class = RampLakeflowConnect
    simulator_source = "ramp"
    sample_records = 5
    replay_config = {
        "environment": "sandbox",
        "access_token": "simulator-fake-access-token",
    }
