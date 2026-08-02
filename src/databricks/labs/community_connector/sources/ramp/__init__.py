"""Ramp source connector."""

from databricks.labs.community_connector.sources.ramp.ramp import (
    RampLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class RampDataSource(LakeflowSource):
    """Spark Python Data Source bound to the Ramp connector."""

    _lakeflow_connect_cls = RampLakeflowConnect
    # Keep the default "lakeflow_connect" format while Unity Catalog injects
    # COMMUNITY connection options.


__all__ = [
    "RampLakeflowConnect",
    "RampDataSource",
]
