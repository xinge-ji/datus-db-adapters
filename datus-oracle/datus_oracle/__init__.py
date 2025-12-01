# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

from .config import OracleConfig
from .connector import OracleConnector

__version__ = "0.1.0"
__all__ = ["OracleConnector", "OracleConfig", "register"]


def register():
    """Register Oracle connector with Datus registry."""
    from datus.tools.db_tools import connector_registry

    connector_registry.register("oracle", OracleConnector)
