# datus-doris

Doris database adapter for Datus.

## Overview

Apache Doris is a high-performance analytical database that uses the MySQL protocol. This adapter extends the MySQL connector with Doris-specific features:

- Catalog-aware metadata handling
- Materialized view support
- Doris-specific metadata queries

## Installation

```bash
pip install datus-doris
```

This will automatically install the required dependencies:
- `datus-agent`
- `datus-mysql` (which includes `datus-sqlalchemy`)

## Usage

The adapter is automatically registered with Datus when installed. Configure your database connection:

```yaml
database:
  type: doris
  host: localhost
  port: 9030
  username: root
  password: your_password
  catalog: internal
  database: your_database
```

Or use programmatically:

```python
from datus_doris import DorisConnector

# Create connector
connector = DorisConnector(
    host="localhost",
    port=9030,
    username="root",
    password="your_password",
    catalog="internal",
    database="mydb",
)

# Use context manager for automatic cleanup
with connector:
    # Test connection
    connector.test_connection()

    # Get catalogs
    catalogs = connector.get_catalogs()
    print(f"Catalogs: {catalogs}")

    # Get databases in catalog
    databases = connector.get_databases(catalog_name="internal")
    print(f"Databases: {databases}")

    # Get tables
    tables = connector.get_tables(catalog_name="internal", database_name="mydb")
    print(f"Tables: {tables}")

    # Get materialized views
    mvs = connector.get_materialized_views(database_name="mydb")
    print(f"Materialized Views: {mvs}")

    # Get materialized views with DDL
    mvs_with_ddl = connector.get_materialized_views_with_ddl(database_name="mydb")
    for mv in mvs_with_ddl:
        print(f"\n{mv['table_name']}:")
        print(mv['definition'])

    # Execute query
    result = connector.execute_query("SELECT * FROM users LIMIT 10")
    print(result.sql_return)
```

## Features

### Doris-Specific Features
- **Catalog-aware metadata**: Handle Doris catalogs while building identifiers
- **Materialized views**: Full support for Doris materialized views
- **Catalog management**: Switch between catalogs seamlessly

### Inherited from MySQL
- Full CRUD operations (SELECT, INSERT, UPDATE, DELETE)
- DDL execution (CREATE, ALTER, DROP)
- Metadata retrieval (tables, views, schemas)
- Sample data extraction
- Multiple result formats (pandas, arrow, csv, list)
- Connection pooling and management

### Fully-Qualified Names

Doris supports three-part names: `catalog.database.table`

```python
# Build full name
full_name = connector.full_name(
    catalog_name="internal",
    database_name="mydb",
    table_name="users",
)
# Result: `internal`.`mydb`.`users`

# Query with full name
result = connector.execute_query(f"SELECT * FROM {full_name} LIMIT 10")
```

## Requirements

- Python >= 3.12
- Apache Doris >= 2.0
- datus-agent >= 0.3.0
- datus-mysql >= 0.1.0

## License

Apache License 2.0

## Related Packages

- `datus-mysql` - MySQL adapter (base for Doris)
- `datus-sqlalchemy` - SQLAlchemy base connector
- `datus-starrocks` - StarRocks adapter
