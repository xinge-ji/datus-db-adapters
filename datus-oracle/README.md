# datus-oracle

Oracle database adapter for Datus Agent built on the shared SQLAlchemy base, using the `python-oracledb` driver.

## Features

- SQLAlchemy-based connection management with service name (`database`) or SID support.
- SQL execution with CSV, pandas, Arrow, and list result formats.
- Schema discovery for tables, views, and materialized views (via SQLAlchemy inspector).
- DDL extraction through `DBMS_METADATA.GET_DDL` is available in Oracle; metadata functions reuse shared base helpers.
- Sample row exploration and streaming CSV iterators.
- Integrated SQLAlchemy/Datus error handling.

## Installation

```bash
pip install datus-oracle
```

## Usage

```python
from datus_oracle import OracleConfig, OracleConnector

config = OracleConfig(
    host="db.example.com",
    port=1521,
    database="FREEPDB1",  # service name
    username="datus",
    password="secret",
)

connector = OracleConnector(config)
result = connector.execute_query("SELECT * FROM HR.EMPLOYEES FETCH FIRST 10 ROWS ONLY", result_format="list")
print(result.sql_return)
connector.close()
```

## Entry Point

The adapter registers itself under the `oracle` key in the `datus.adapters` entry point group, allowing Datus Agent to load it automatically.
