# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

import os
import uuid
from typing import Generator

import pytest
from datus.tools.db_tools.mixins import CatalogSupportMixin, MaterializedViewSupportMixin
from datus.utils.exceptions import DatusException, ErrorCode
from datus_doris import DorisConfig, DorisConnector


@pytest.fixture
def config() -> DorisConfig:
    """Create Doris configuration from environment or defaults."""
    return DorisConfig(
        host=os.getenv("DORIS_HOST", "localhost"),
        port=int(os.getenv("DORIS_PORT", "9030")),
        username=os.getenv("DORIS_USER", "root"),
        password=os.getenv("DORIS_PASSWORD", ""),
        catalog=os.getenv("DORIS_CATALOG", "internal"),
        database=os.getenv("DORIS_DATABASE", "quickstart"),
    )


@pytest.fixture
def connector(config: DorisConfig) -> Generator[DorisConnector, None, None]:
    """Create and cleanup Doris connector."""
    conn = DorisConnector(config)
    yield conn
    conn.close()


# ==================== Mixin Tests ====================


def test_connector_implements_catalog_mixin(connector: DorisConnector):
    """Verify Doris connector implements CatalogSupportMixin."""
    assert isinstance(connector, CatalogSupportMixin)


def test_connector_implements_materialized_view_mixin(connector: DorisConnector):
    """Verify Doris connector implements MaterializedViewSupportMixin."""
    assert isinstance(connector, MaterializedViewSupportMixin)


# ==================== Connection Tests ====================


def test_connection_with_config_object(config: DorisConfig):
    """Test connection using config object."""
    conn = DorisConnector(config)
    assert conn.test_connection()
    conn.close()


def test_connection_with_dict():
    """Test connection using dict config."""
    conn = DorisConnector(
        {
            "host": os.getenv("DORIS_HOST", "localhost"),
            "port": int(os.getenv("DORIS_PORT", "9030")),
            "username": os.getenv("DORIS_USER", "root"),
            "password": os.getenv("DORIS_PASSWORD", ""),
        }
    )
    assert conn.test_connection()
    conn.close()


def test_context_manager(config: DorisConfig):
    """Test connector as context manager."""
    with DorisConnector(config) as conn:
        assert conn.test_connection()


# ==================== Catalog Tests (CatalogSupportMixin) ====================


def test_get_catalogs(connector: DorisConnector):
    """Test getting list of catalogs."""
    catalogs = connector.get_catalogs()
    assert len(catalogs) > 0
    assert connector.default_catalog() in catalogs


def test_default_catalog(connector: DorisConnector):
    """Test default catalog."""
    assert connector.default_catalog() == "internal"


def test_switch_catalog(connector: DorisConnector):
    """Test switching catalogs."""
    original_catalog = connector.catalog_name
    catalogs = connector.get_catalogs()

    if len(catalogs) > 1:
        target_catalog = [c for c in catalogs if c != original_catalog][0]
        connector.switch_catalog(target_catalog)
        assert connector.catalog_name == target_catalog

        connector.switch_catalog(original_catalog)
        assert connector.catalog_name == original_catalog


# ==================== Database Tests ====================


def test_get_databases(connector: DorisConnector):
    """Test getting list of databases."""
    databases = connector.get_databases()
    assert len(databases) > 0


# ==================== Table Metadata Tests ====================


def test_get_tables(connector: DorisConnector):
    """Test getting table list."""
    tables = connector.get_tables()
    assert isinstance(tables, list)


def test_get_tables_with_ddl(connector: DorisConnector, config: DorisConfig):
    """Test getting tables with DDL."""
    tables = connector.get_tables_with_ddl(catalog_name=config.catalog)

    if len(tables) > 0:
        table = tables[0]
        assert "table_name" in table
        assert "definition" in table
        assert table["table_type"] == "table"
        assert "database_name" in table
        assert table["schema_name"] == ""
        assert table["catalog_name"] == config.catalog
        assert "identifier" in table
        assert len(table["identifier"].split(".")) == 3


# ==================== View Tests ====================


def test_get_views(connector: DorisConnector):
    """Test getting view list."""
    views = connector.get_views()
    assert isinstance(views, list)


def test_get_views_with_ddl(connector: DorisConnector, config: DorisConfig):
    """Test getting views with DDL."""
    views = connector.get_views_with_ddl(catalog_name=config.catalog)

    if len(views) > 0:
        view = views[0]
        assert "table_name" in view
        assert "definition" in view
        assert view["table_type"] == "view"
        assert "database_name" in view
        assert view["schema_name"] == ""
        assert "catalog_name" in view

        identifier_parts = view["identifier"].split(".")
        assert len(identifier_parts) == 3
        assert identifier_parts[0] == view["catalog_name"]
        assert identifier_parts[1] == view["database_name"]
        assert identifier_parts[2] == view["table_name"]


# ==================== Materialized View Tests (MaterializedViewSupportMixin) ====================


def test_get_materialized_views(connector: DorisConnector, config: DorisConfig):
    """Test getting materialized view list."""
    mvs = connector.get_materialized_views(catalog_name=config.catalog)
    assert isinstance(mvs, list)


def test_get_materialized_views_with_ddl(connector: DorisConnector):
    """Test getting materialized views with DDL."""
    mvs = connector.get_materialized_views_with_ddl()

    if len(mvs) > 0:
        mv = mvs[0]
        assert "table_name" in mv
        assert "definition" in mv
        assert mv["table_type"] == "mv"
        assert "database_name" in mv
        assert mv["schema_name"] == ""
        assert "catalog_name" in mv

        identifier_parts = mv["identifier"].split(".")
        assert len(identifier_parts) == 3


def test_show_create_materialized_view(monkeypatch, connector: DorisConnector):
    """Fallback to SHOW CREATE MATERIALIZED VIEW after async MV error hint."""

    calls = []

    def fake_execute(sql):
        calls.append(sql)

        import pandas as pd

        if len(calls) == 1:
            assert "SHOW CREATE TABLE" in sql
            raise Exception(
                "errCode = 2, detailMessage = not support async materialized view, please use `show create materialized view`"
            )

        assert "SHOW CREATE MATERIALIZED VIEW" in sql
        return pd.DataFrame(
            [["mv", "CREATE MATERIALIZED VIEW mv AS ..."]],
            columns=["Materialized View", "Create Materialized View"],
        )

    monkeypatch.setattr(connector, "_execute_pandas", fake_execute)

    ddl = connector._show_create("`internal`.`db`.`mv`", "TABLE")

    assert ddl == "CREATE MATERIALIZED VIEW mv AS ..."
    assert len(calls) == 2


def test_materialized_view_detection_filters(monkeypatch, connector: DorisConnector):
    """Use SHOW CREATE TABLE errors to classify materialized views from BASE TABLE metadata."""

    import pandas as pd

    # Avoid real connections
    monkeypatch.setattr(connector, "_before_metadata_query", lambda catalog_name="", database_name="": None)
    monkeypatch.setattr(connector, "connect", lambda: None)

    # Simulate BASE TABLE entries for a table and a materialized view
    monkeypatch.setattr(
        connector,
        "_execute_pandas",
        lambda sql: pd.DataFrame(
            [["db", "fact_table"], ["db", "sales_mv"]], columns=["TABLE_SCHEMA", "TABLE_NAME"]
        ),
    )

    # Only the second object is a materialized view
    monkeypatch.setattr(
        connector,
        "_is_materialized_view",
        lambda full_name: full_name.endswith("`sales_mv`"),
    )

    tables = connector.get_tables(catalog_name="internal", database_name="db")
    mvs = connector.get_materialized_views(catalog_name="internal", database_name="db")

    assert tables == ["fact_table"]
    assert mvs == ["sales_mv"]


# ==================== Sample Data Tests ====================
