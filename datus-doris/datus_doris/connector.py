# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

from typing import Any, Dict, List, Union, override

from datus.schemas.base import TABLE_TYPE
from datus.tools.db_tools.base import list_to_in_str
from datus.tools.db_tools.mixins import CatalogSupportMixin, MaterializedViewSupportMixin
from datus.utils.constants import DBType
from datus.utils.loggings import get_logger
from datus_mysql import MySQLConnector
from datus_mysql.connector import TableMetadataNames

from .config import DorisConfig

logger = get_logger(__name__)


# Doris metadata configuration uses information_schema.TABLES for all object types
DORIS_METADATA_DICT: Dict[TABLE_TYPE, TableMetadataNames] = {
    "table": TableMetadataNames(
        show_table="TABLES", show_create_table="TABLE", info_table="TABLES", table_types=["BASE TABLE"]
    ),
    "view": TableMetadataNames(
        show_table="TABLES",
        show_create_table="VIEW",
        info_table="TABLES",
        table_types=["VIEW"],
    ),
    "mv": TableMetadataNames(
        show_table="TABLES", show_create_table="MATERIALIZED VIEW", info_table="TABLES", table_types=["BASE TABLE"]
    ),
}


def _get_metadata_config(table_type: TABLE_TYPE) -> TableMetadataNames:
    """Get Doris metadata configuration for the given table type."""

    if table_type not in DORIS_METADATA_DICT:
        raise ValueError(f"Invalid table type '{table_type}' for Doris")

    return DORIS_METADATA_DICT[table_type]


def _is_async_mv_hint(error: Exception) -> bool:
    """Return True if the error message indicates an async materialized view."""

    error_lower = str(error).lower()
    return "not support async materialized view" in error_lower and "show create materialized view" in error_lower


class DorisConnector(MySQLConnector, CatalogSupportMixin, MaterializedViewSupportMixin):
    """
    Doris database connector.

    Doris uses the MySQL protocol and supports catalogs and materialized views.
    This connector implements CatalogSupportMixin and MaterializedViewSupportMixin.
    """

    def __init__(self, config: Union[DorisConfig, dict]):
        """
        Initialize Doris connector.

        Args:
            config: DorisConfig object or dict with configuration
        """
        if isinstance(config, dict):
            config = DorisConfig(**config)
        elif not isinstance(config, DorisConfig):
            raise TypeError(f"config must be DorisConfig or dict, got {type(config)}")

        self.doris_config = config

        from datus_mysql import MySQLConfig

        mysql_config = MySQLConfig(
            host=config.host,
            port=config.port,
            username=config.username,
            password=config.password,
            database=config.database or "",
            charset=config.charset,
            autocommit=config.autocommit,
            timeout_seconds=config.timeout_seconds,
        )
        super().__init__(mysql_config)

        self.catalog_name = config.catalog

        # Override dialect to Doris
        self.dialect = DBType.DORIS

    # ==================== Context Manager Support ====================

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.close()
        return False  # Don't suppress exceptions

    # ==================== Catalog Management (CatalogSupportMixin) ====================

    @override
    def default_catalog(self) -> str:
        """Doris default catalog."""
        return "internal"

    @override
    def get_catalogs(self) -> List[str]:
        """Get list of catalogs."""
        result = self._execute_pandas("SHOW CATALOGS")
        if result.empty:
            return []
        return result["Catalog"].tolist()

    @override
    def switch_catalog(self, catalog_name: str) -> None:
        """Switch to a different catalog.

        Args:
            catalog_name: Name of the catalog to switch to
        """
        self.switch_context(catalog_name=catalog_name)
        self.catalog_name = catalog_name

    def reset_catalog_to_default(self, catalog: str) -> str:
        """Reset the catalog to the default catalog if it is not set or is 'def'."""
        if not catalog or catalog == "def":
            return self.default_catalog()
        return catalog

    def _resolved_catalog(self, catalog_name: str = "") -> str:
        """Return the effective catalog, preferring the configured catalog when unset."""

        if catalog_name:
            return self.reset_catalog_to_default(catalog_name)

        if self.catalog_name:
            return self.catalog_name

        return self.default_catalog()

    def _before_metadata_query(self, catalog_name: str = "", database_name: str = "") -> None:
        """Switch catalog before metadata queries if needed."""
        target_catalog = self._resolved_catalog(catalog_name)
        if target_catalog and target_catalog != self.catalog_name:
            self.switch_context(catalog_name=target_catalog)

    # ==================== Metadata Retrieval ====================

    def _get_metadata(
        self,
        table_type: str = "table",
        catalog_name: str = "",
        database_name: str = "",
    ) -> List[Dict[str, str]]:
        """
        Get metadata for tables/views with catalog support.

        Args:
            table_type: Type of object (table, view, mv)
            catalog_name: Catalog name
            database_name: Database name to query

        Returns:
            List of metadata dictionaries with catalog_name properly set
        """
        current_catalog = self._resolved_catalog(catalog_name)

        self._before_metadata_query(catalog_name=current_catalog, database_name=database_name)

        self.connect()
        database_name = database_name or self.database_name

        if database_name:
            where = f"TABLE_SCHEMA = '{database_name}'"
        else:
            where = f"{list_to_in_str('TABLE_SCHEMA not in', list(self._sys_databases()))}"

        metadata_config = _get_metadata_config(table_type)
        type_filter = list_to_in_str("and TABLE_TYPE in ", metadata_config.table_types)

        query = (
            f"SELECT TABLE_SCHEMA, TABLE_NAME "
            f"FROM information_schema.{metadata_config.info_table} "
            f"WHERE {where} {type_filter}"
        )

        query_result = self._execute_pandas(query)

        filtered_result = []
        for i in range(len(query_result)):
            db_name = query_result["TABLE_SCHEMA"][i]
            tb_name = query_result["TABLE_NAME"][i]

            full_name = self.full_name(
                catalog_name=current_catalog, database_name=db_name, table_name=tb_name
            )

            # Doris reports both tables and materialized views as BASE TABLE. Use SHOW
            # CREATE TABLE error hints to distinguish materialized views.
            if table_type in {"table", "mv"}:
                try:
                    is_mv = self._is_materialized_view(full_name)
                except Exception as e:
                    logger.warning(f"Could not determine if {full_name} is a materialized view: {e}")
                    is_mv = False

                if table_type == "mv" and not is_mv:
                    continue
                if table_type == "table" and is_mv:
                    continue

            item = {
                "identifier": self.identifier(
                    catalog_name=current_catalog, database_name=db_name, table_name=tb_name
                ),
                "catalog_name": current_catalog,
                "schema_name": "",
                "database_name": db_name,
                "table_name": tb_name,
                "table_type": table_type,
            }

            filtered_result.append(item)

        return filtered_result

    @override
    def get_tables(self, catalog_name: str = "", database_name: str = "", schema_name: str = "") -> List[str]:
        """Get list of table names."""
        result = self._get_metadata(table_type="table", catalog_name=catalog_name, database_name=database_name)
        return [table["table_name"] for table in result]

    @override
    def get_views(self, catalog_name: str = "", database_name: str = "", schema_name: str = "") -> List[str]:
        """Get list of view names."""
        try:
            result = self._get_metadata(table_type="view", catalog_name=catalog_name, database_name=database_name)
            return [view["table_name"] for view in result]
        except Exception as e:
            logger.warning(f"Failed to get views: {e}")
            return []

    def get_materialized_views(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = ""
    ) -> List[str]:
        """Get list of materialized view names."""
        try:
            result = self._get_metadata(table_type="mv", catalog_name=catalog_name, database_name=database_name)
            return [mv["table_name"] for mv in result]
        except Exception as e:
            logger.warning(f"Failed to get materialized views: {e}")
            return []

    @override
    def _get_objects_with_ddl(
        self,
        table_type: TABLE_TYPE = "table",
        tables: List[str] | None = None,
        catalog_name: str = "",
        database_name: str = "",
    ) -> List[Dict[str, str]]:
        """Get metadata with DDL statements using Doris metadata configuration."""

        result = []
        filter_tables = self._reset_filter_tables(tables, catalog_name, database_name)
        metadata_config = _get_metadata_config(table_type)

        for meta in self._get_metadata(table_type, catalog_name, database_name):
            full_name = self.full_name(
                catalog_name=meta.get("catalog_name", ""),
                database_name=meta["database_name"],
                table_name=meta["table_name"],
            )

            if filter_tables and full_name not in filter_tables:
                continue

            try:
                ddl = self._show_create(full_name, metadata_config.show_create_table)
            except Exception as e:
                logger.warning(f"Could not get DDL for {full_name}: {e}")
                ddl = f"-- DDL not available for {full_name}"

            result.append({**meta, "definition": ddl})

        return result

    def get_materialized_views_with_ddl(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = ""
    ) -> List[Dict[str, str]]:
        """
        Get materialized views with DDL definitions.

        Args:
            catalog_name: Catalog name
            database_name: Database name
            schema_name: Schema name (unused in Doris)

        Returns:
            List of materialized view metadata with DDL
        """
        current_catalog = self._resolved_catalog(catalog_name)

        self._before_metadata_query(catalog_name=current_catalog, database_name=database_name)
        try:
            mv_metadata = self._get_metadata(table_type="mv", catalog_name=current_catalog, database_name=database_name)
        except Exception as e:
            logger.warning(f"Failed to list materialized views for DDL retrieval: {e}")
            return []

        mv_list = []
        for mv in mv_metadata:
            identifier = mv["identifier"]
            full_name = self.full_name(
                catalog_name=current_catalog, database_name=mv["database_name"], table_name=mv["table_name"]
            )

            try:
                definition = self._show_create(full_name, "MATERIALIZED VIEW")
            except Exception as e:  # pragma: no cover - best-effort retrieval
                logger.warning(f"Failed to get DDL for {identifier}: {e}")
                definition = f"-- DDL not available for {identifier}"

            mv_list.append({**mv, "definition": definition})

        return mv_list

    @override
    def _show_create(self, full_name: str, create_type: str) -> str:
        """
        Execute SHOW CREATE with Doris-specific handling for materialized views.

        Doris may return an async materialized view error when using TABLE
        metadata, instructing callers to use ``SHOW CREATE MATERIALIZED VIEW``.
        In that case, retry with the materialized view command; otherwise
        respect the requested ``create_type``.
        """
        try:
            return super()._show_create(full_name, create_type)
        except Exception as e:
            if _is_async_mv_hint(e):
                logger.debug(f"Retrying SHOW CREATE MATERIALIZED VIEW for {full_name} after async MV hint: {e}")
                return super()._show_create(full_name, "MATERIALIZED VIEW")

            raise

    def _is_materialized_view(self, full_name: str) -> bool:
        """
        Determine whether a table name refers to a Doris materialized view.

        Doris reports materialized views as ``BASE TABLE`` in metadata. Attempt
        ``SHOW CREATE TABLE`` and interpret the async materialized view error to
        classify the object as a materialized view.
        """

        try:
            super()._show_create(full_name, "TABLE")
            return False
        except Exception as e:
            if _is_async_mv_hint(e):
                return True

            raise

    # ==================== Database Management ====================

    @override
    def get_databases(self, catalog_name: str = "", include_sys: bool = False) -> List[str]:
        """Get list of databases in the catalog."""
        return super().get_databases(catalog_name, include_sys=include_sys)

    # ==================== Full Name Construction ====================

    @override
    def full_name(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = "", table_name: str = ""
    ) -> str:
        """
        Build fully-qualified table name with catalog support.

        Doris format: `catalog`.`database`.`table`
        """
        catalog_name = self._resolved_catalog(catalog_name)

        if catalog_name:
            if database_name:
                return f"`{catalog_name}`.`{database_name}`.`{table_name}`"
            else:
                return f"`{table_name}`"
        else:
            if database_name:
                return f"`{database_name}`.`{table_name}`"
            return f"`{table_name}`"

    @override
    def _sqlalchemy_schema(self, catalog_name: str = "", database_name: str = "", schema_name: str = "") -> str:
        """Get schema name for SQLAlchemy Inspector with catalog support."""
        database_name = database_name or self.database_name

        if self.support_catalog():
            catalog_name = catalog_name or self.catalog_name or self.default_catalog()
            if database_name:
                return f"{catalog_name}.{database_name}"
            return None
        else:
            return database_name if database_name else None

    # ==================== Connection Cleanup ====================

    @override
    def close(self):
        """
        Close connection with special handling for PyMySQL cleanup errors.

        Doris may trigger PyMySQL struct.pack errors during cleanup,
        which we safely ignore.
        """
        try:
            super().close()
        except Exception as e:
            error_str = str(e)

            pymysql_errors = ["struct.error", "struct.pack", "COMMAND.COM_QUIT", "required argument is not an integer"]

            if any(err in error_str for err in pymysql_errors):
                logger.debug(f"Ignoring PyMySQL cleanup error: {e}")

                if hasattr(self, "connection"):
                    self.connection = None
                if hasattr(self, "engine"):
                    try:
                        if self.engine:
                            self.engine.dispose()
                    except Exception:
                        pass
                    finally:
                        self.engine = None
            else:
                logger.error(f"Unexpected close error: {e}")
                raise

    # ==================== Utility Methods ====================

    def to_dict(self) -> Dict[str, Any]:
        """Convert connector to serializable dictionary."""
        return {
            "db_type": DBType.DORIS,
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "catalog": self.catalog_name,
            "database": self.database_name,
        }

    def get_type(self) -> str:
        """Return the database type."""
        return DBType.DORIS

    @override
    def test_connection(self) -> bool:
        """Test the database connection with proper cleanup."""
        try:
            return super().test_connection()
        finally:
            try:
                self.close()
            except Exception as e:
                logger.debug(f"Ignoring cleanup error during test: {e}")
