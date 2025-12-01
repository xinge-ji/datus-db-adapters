# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Union, override
from urllib.parse import quote_plus

import oracledb
from datus.schemas.base import TABLE_TYPE
from datus.utils.constants import DBType
from datus.utils.loggings import get_logger
from datus_sqlalchemy import SQLAlchemyConnector
from sqlalchemy import text

from .config import OracleConfig

logger = get_logger(__name__)


def _quote_identifier(identifier: str) -> str:
    """Double-quote an identifier, escaping internal quotes."""
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


class OracleConnector(SQLAlchemyConnector):
    """Oracle database connector built on the shared SQLAlchemy base."""

    def __init__(self, config: Union[OracleConfig, Dict[str, Any]]):
        if isinstance(config, dict):
            config = OracleConfig(**config)
        elif not isinstance(config, OracleConfig):
            raise TypeError(f"config must be OracleConfig or dict, got {type(config)}")

        self.config = config
        self._ensure_thick_mode()
        connection_string = self._build_connection_string(config)

        super().__init__(connection_string, dialect=DBType.ORACLE, timeout_seconds=config.timeout_seconds)
        self.database_name = config.database or config.sid or ""
        self.schema_name = (config.schema_name or config.username).upper()
        self.catalog_name = ""

    def _ensure_thick_mode(self):
        """
        Initialize Oracle client in thick mode if the driver is running in thin mode.

        Oracle client libraries must be installed locally; we fail soft to allow environments
        without Instant Client to continue using thin mode.
        """
        try:
            is_thin = getattr(oracledb, "is_thin_mode", lambda: False)()
        except Exception:
            return

        if not is_thin:
            return

        init_kwargs: Dict[str, Any] = {}
        if self.config.client_lib_dir:
            init_kwargs["lib_dir"] = self.config.client_lib_dir

        try:
            oracledb.init_oracle_client(**init_kwargs)
            logger.info("Initialized Oracle thick client mode")
        except Exception as exc:
            logger.warning(f"Failed to initialize Oracle thick client, continuing in thin mode: {exc}")

    @staticmethod
    def _build_connection_string(config: OracleConfig) -> str:
        """Build a SQLAlchemy Oracle connection string."""
        encoded_password = quote_plus(config.password) if config.password else ""
        auth = f"{config.username}:{encoded_password}@"
        base = f"oracle+oracledb://{auth}{config.host}:{config.port}"

        if config.database:
            return f"{base}/?service_name={quote_plus(config.database)}"
        return f"{base}/?sid={quote_plus(config.sid)}"

    # ==================== System Resources ====================

    @override
    def _sys_schemas(self) -> Set[str]:
        """System schemas to filter out."""
        return {
            "ANONYMOUS",
            "APEX_PUBLIC_USER",
            "APPQOSSYS",
            "AUDSYS",
            "CTXSYS",
            "DBSNMP",
            "DIP",
            "DVF",
            "DVSYS",
            "FLOWS_FILES",
            "GSMADMIN_INTERNAL",
            "LBACSYS",
            "MDSYS",
            "OLAPSYS",
            "ORDPLUGINS",
            "ORDSYS",
            "OUTLN",
            "REMOTE_SCHEDULER_AGENT",
            "SI_INFORMTN_SCHEMA",
            "SYS",
            "SYSBACKUP",
            "SYSDG",
            "SYSKM",
            "SYSTEM",
            "WMSYS",
            "XDB",
        }

    # ==================== Context Management ====================

    @override
    def do_switch_context(self, catalog_name: str = "", database_name: str = "", schema_name: str = ""):
        """Switch schema context using ALTER SESSION."""
        target_schema = (schema_name or self.schema_name).upper()
        if not target_schema:
            return

        self.connect()
        with self.connection.begin():
            self.connection.execute(text(f'ALTER SESSION SET CURRENT_SCHEMA = "{target_schema}"'))
        self.schema_name = target_schema

    # ==================== Metadata Helpers ====================

    @override
    def _sqlalchemy_schema(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = ""
    ) -> Optional[str]:
        """Return schema name for SQLAlchemy inspector."""
        return (schema_name or self.schema_name or self.config.username).upper()

    @override
    def get_databases(self, catalog_name: str = "", include_sys: bool = False) -> List[str]:
        """Oracle uses a single database instance; return the configured service or SID."""
        return [self.database_name] if self.database_name else []

    # ==================== Object Metadata ====================

    def _list_objects(
        self,
        table_type: TABLE_TYPE,
        catalog_name: str = "",
        database_name: str = "",
        schema_name: str = "",
        tables: Optional[List[str]] = None,
    ) -> List[Dict[str, str]]:
        """List tables/views/materialized views from ALL_* views."""
        if table_type == "full":
            combined: List[Dict[str, str]] = []
            for sub_type in ("table", "view", "mv"):
                combined.extend(self._list_objects(sub_type, catalog_name, database_name, schema_name, tables))
            return combined
        table_type = "mv" if table_type == "materialized_view" else table_type
        meta_map = {
            "table": ("ALL_TABLES", "TABLE_NAME"),
            "view": ("ALL_VIEWS", "VIEW_NAME"),
            "mv": ("ALL_MVIEWS", "MVIEW_NAME"),
        }
        if table_type not in meta_map:
            return []

        source_table, name_column = meta_map[table_type]
        schema = (schema_name or self.schema_name or self.config.username).upper()

        sql = f"SELECT OWNER, {name_column} AS OBJECT_NAME FROM {source_table} WHERE 1=1"
        params: Dict[str, Any] = {}

        if schema:
            sql += " AND OWNER = :owner"
            params["owner"] = schema
        else:
            sys_schemas = ", ".join(f":sys_{i}" for i in range(len(self._sys_schemas())))
            sql += f" AND OWNER NOT IN ({sys_schemas})"
            for i, name in enumerate(sorted(self._sys_schemas())):
                params[f"sys_{i}"] = name

        if tables:
            placeholders = ", ".join(f":tbl_{idx}" for idx in range(len(tables)))
            sql += f" AND {name_column} IN ({placeholders})"
            for idx, table_name in enumerate(tables):
                params[f"tbl_{idx}"] = table_name.upper()

        sql += f" ORDER BY OWNER, {name_column}"

        self.connect()
        result = self.connection.execute(text(sql), params).fetchall()

        output = []
        for owner, object_name in result:
            owner_name = str(owner)
            obj_name = str(object_name)
            output.append(
                {
                    "catalog_name": catalog_name,
                    "database_name": database_name or self.database_name,
                    "schema_name": owner_name,
                    "table_name": obj_name,
                    "table_type": table_type,
                    "identifier": self.identifier(
                        catalog_name=catalog_name,
                        database_name=database_name or self.database_name,
                        schema_name=owner_name,
                        table_name=obj_name,
                    ),
                }
            )
        return output

    def _fetch_object_ddl(self, object_type: str, owner: str, name: str) -> str:
        """Fetch DDL via DBMS_METADATA."""
        ddl_sql = text("SELECT DBMS_METADATA.GET_DDL(:object_type, :name) FROM dual")
        try:
            self.connect()
            row = self.connection.execute(
                ddl_sql, {"object_type": object_type, "name": name, "owner": owner}
            ).fetchone()
            if not row:
                return f"-- DDL not available for {owner}.{name}"
            ddl = row[0]
            return ddl.read() if hasattr(ddl, "read") else ddl
        except Exception as exc:
            logger.warning(f"Failed to get DDL for {owner}.{name}: {exc}")
            return f"-- DDL not available for {owner}.{name}: {exc}"

    def _objects_with_ddl(
        self,
        table_type: TABLE_TYPE,
        catalog_name: str = "",
        database_name: str = "",
        schema_name: str = "",
        tables: Optional[List[str]] = None,
    ) -> List[Dict[str, str]]:
        entries = self._list_objects(table_type, catalog_name, database_name, schema_name, tables)
        if not entries:
            return []

        object_type = {
            "table": "TABLE",
            "view": "VIEW",
            "mv": "MATERIALIZED VIEW",
        }.get(table_type, "TABLE")

        for entry in entries:
            entry["definition"] = self._fetch_object_ddl(object_type, entry["schema_name"], entry["table_name"])
        return entries

    @override
    def get_tables(self, catalog_name: str = "", database_name: str = "", schema_name: str = "") -> List[str]:
        return [meta["table_name"] for meta in self._list_objects("table", catalog_name, database_name, schema_name)]

    @override
    def get_views(self, catalog_name: str = "", database_name: str = "", schema_name: str = "") -> List[str]:
        return [meta["table_name"] for meta in self._list_objects("view", catalog_name, database_name, schema_name)]

    def get_materialized_views(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = ""
    ) -> List[str]:
        return [meta["table_name"] for meta in self._list_objects("mv", catalog_name, database_name, schema_name)]

    @override
    def get_tables_with_ddl(
        self,
        catalog_name: str = "",
        database_name: str = "",
        schema_name: str = "",
        tables: Optional[List[str]] = None,
    ) -> List[Dict[str, str]]:
        return self._objects_with_ddl("table", catalog_name, database_name, schema_name, tables)

    @override
    def get_views_with_ddl(
        self,
        catalog_name: str = "",
        database_name: str = "",
        schema_name: str = "",
        tables: Optional[List[str]] = None,
    ) -> List[Dict[str, str]]:
        return self._objects_with_ddl("view", catalog_name, database_name, schema_name, tables)

    def get_materialized_views_with_ddl(
        self,
        catalog_name: str = "",
        database_name: str = "",
        schema_name: str = "",
        tables: Optional[List[str]] = None,
    ) -> List[Dict[str, str]]:
        return self._objects_with_ddl("mv", catalog_name, database_name, schema_name, tables)

    def get_schema(
        self,
        catalog_name: str = "",
        database_name: str = "",
        schema_name: str = "",
        table_name: str = "",
        table_type: str = "table",
    ) -> List[Dict[str, Any]]:
        """Get column metadata for a table/view/mv."""
        if not table_name:
            return []

        owner = (schema_name or self.schema_name or self.config.username).upper()
        sql = text(
            """
            SELECT
                cols.COLUMN_ID,
                cols.COLUMN_NAME,
                cols.DATA_TYPE,
                cols.DATA_LENGTH,
                cols.DATA_PRECISION,
                cols.DATA_SCALE,
                cols.NULLABLE,
                cols.DATA_DEFAULT,
                pk.COLUMN_NAME AS PK_COLUMN
            FROM ALL_TAB_COLUMNS cols
            LEFT JOIN (
                SELECT acc.COLUMN_NAME
                FROM ALL_CONSTRAINTS ac
                JOIN ALL_CONS_COLUMNS acc
                    ON ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
                   AND ac.OWNER = acc.OWNER
                WHERE ac.CONSTRAINT_TYPE = 'P'
                  AND ac.OWNER = :owner
                  AND ac.TABLE_NAME = :table_name
            ) pk
            ON cols.COLUMN_NAME = pk.COLUMN_NAME
            WHERE cols.OWNER = :owner
              AND cols.TABLE_NAME = :table_name
            ORDER BY cols.COLUMN_ID
            """
        )

        self.connect()
        rows = self.connection.execute(sql, {"owner": owner, "table_name": table_name.upper()}).fetchall()

        result: List[Dict[str, Any]] = []
        for column_id, name, data_type, data_length, data_precision, data_scale, nullable, default, pk in rows:
            type_repr = data_type
            if data_precision is not None:
                if data_scale is not None:
                    type_repr = f"{data_type}({int(data_precision)},{int(data_scale)})"
                else:
                    type_repr = f"{data_type}({int(data_precision)})"
            elif data_length is not None and data_length > 0:
                type_repr = f"{data_type}({int(data_length)})"

            result.append(
                {
                    "cid": int(column_id) if column_id is not None else len(result),
                    "name": name,
                    "type": type_repr,
                    "nullable": str(nullable).upper() == "Y",
                    "default_value": default,
                    "pk": pk is not None,
                }
            )
        return result

    def get_sample_rows(
        self,
        tables: Optional[List[str]] = None,
        top_n: int = 5,
        catalog_name: str = "",
        database_name: str = "",
        schema_name: str = "",
        table_type: TABLE_TYPE = "table",
    ) -> List[Dict[str, Any]]:
        """Get sample rows using FETCH FIRST syntax."""
        schema = (schema_name or self.schema_name or self.config.username).upper()
        if tables:
            targets = [{"schema_name": schema, "table_name": table} for table in tables]
        else:
            targets = self._list_objects(table_type, catalog_name, database_name, schema)

        result: List[Dict[str, Any]] = []
        if not targets:
            return result

        for entry in targets:
            owner = entry.get("schema_name") or schema
            table = entry["table_name"]
            full_name = self.full_name(
                catalog_name=catalog_name,
                database_name=database_name or self.database_name,
                schema_name=owner,
                table_name=table,
            )
            query = f"SELECT * FROM {full_name} WHERE ROWNUM <= {top_n}"
            df = self._execute_pandas(query)
            if not df.empty:
                result.append(
                    {
                        "identifier": self.identifier(
                            catalog_name=catalog_name,
                            database_name=database_name or self.database_name,
                            schema_name=owner,
                            table_name=table,
                        ),
                        "catalog_name": catalog_name,
                        "database_name": database_name or self.database_name,
                        "schema_name": owner,
                        "table_name": table,
                        "table_type": table_type,
                        "sample_rows": df.to_csv(index=False),
                    }
                )
        return result

    # ==================== Utility Methods ====================

    @override
    def full_name(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = "", table_name: str = ""
    ) -> str:
        """Build fully-qualified table name."""
        if schema_name:
            return f"{_quote_identifier(schema_name)}.{_quote_identifier(table_name)}"
        return _quote_identifier(table_name)

    @override
    def test_connection(self) -> bool:
        """Test the database connection."""
        opened_here = self.connection is None
        try:
            self.connect()
            self.connection.execute("SELECT 1 FROM DUAL").fetchone()
            return True
        except Exception as e:
            raise DatusException(
                ErrorCode.DB_CONNECTION_FAILED,
                message_args={"error_message": str(e)},
            ) from e
        finally:
            if opened_here:
                self.close()
