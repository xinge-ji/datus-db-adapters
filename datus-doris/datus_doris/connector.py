# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

from typing import Any, Dict, List, Union, override

from datus.tools.db_tools.base import list_to_in_str
from datus.tools.db_tools.mixins import CatalogSupportMixin, MaterializedViewSupportMixin
from datus.utils.constants import DBType
from datus.utils.loggings import get_logger
from datus_mysql import MySQLConnector

from .config import DorisConfig

logger = get_logger(__name__)


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

    def _before_metadata_query(self, catalog_name: str = "", database_name: str = "") -> None:
        """Switch catalog before metadata queries if needed."""
        target_catalog = catalog_name or self.catalog_name or self.default_catalog()
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
        current_catalog = self.reset_catalog_to_default(catalog_name or self.catalog_name)

        self._before_metadata_query(catalog_name=current_catalog, database_name=database_name)

        result = super()._get_metadata(table_type, catalog_name, database_name)

        filtered_result = []
        for item in result:
            if "catalog_name" in item and item["catalog_name"] and item["catalog_name"] != current_catalog:
                continue

            item["catalog_name"] = current_catalog
            item["identifier"] = self.identifier(
                catalog_name=current_catalog, database_name=item["database_name"], table_name=item["table_name"]
            )
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
        current_catalog = self.reset_catalog_to_default(catalog_name or self.catalog_name)

        self._before_metadata_query(catalog_name=current_catalog, database_name=database_name)

        query_sql = (
            "SELECT TABLE_SCHEMA, TABLE_NAME, MATERIALIZED_VIEW_DEFINITION "
            "FROM information_schema.materialized_views"
        )

        if database_name:
            query_sql = f"{query_sql} WHERE TABLE_SCHEMA = '{database_name}'"
        else:
            ignore_dbs = list(self._sys_databases())
            query_sql = f"{query_sql} {list_to_in_str('WHERE TABLE_SCHEMA NOT IN', ignore_dbs)}"

        result = self._execute_pandas(query_sql)

        mv_list = []
        for i in range(len(result)):
            mv_list.append(
                {
                    "identifier": self.identifier(
                        catalog_name=current_catalog,
                        database_name=str(result["TABLE_SCHEMA"][i]),
                        table_name=str(result["TABLE_NAME"][i]),
                    ),
                    "catalog_name": current_catalog,
                    "database_name": result["TABLE_SCHEMA"][i],
                    "schema_name": "",
                    "table_name": result["TABLE_NAME"][i],
                    "definition": result["MATERIALIZED_VIEW_DEFINITION"][i],
                    "table_type": "mv",
                }
            )

        return mv_list

    @override
    def _show_create(self, full_name: str, create_type: str) -> str:
        """
        Execute SHOW CREATE with Doris-specific fallback for materialized views.

        Doris returns an error for async materialized views when queried via
        ``SHOW CREATE TABLE``. If we detect that message, retry with the
        MATERIALIZED VIEW variant so DDL retrieval succeeds.
        """
        try:
            return super()._show_create(full_name, create_type)
        except Exception as e:
            error_msg = str(e).lower()

            if "not support async materialized view" in error_msg or "show create materialized view" in error_msg:
                try:
                    return super()._show_create(full_name, "MATERIALIZED VIEW")
                except Exception as mv_error:
                    logger.warning(f"Fallback SHOW CREATE MATERIALIZED VIEW failed for {full_name}: {mv_error}")

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
        catalog_name = self.reset_catalog_to_default(catalog_name)

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
