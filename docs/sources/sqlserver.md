# SQL Server
The SQL Server source will load and save data to Microsoft SQL Server

## Source Configuration
```yaml
sources:
  - name: sqlserver
    type: Sqlserver
    connection:
      settings:
        # The pyodbc/pymysql connection string
        connection_string: "mssql://user:password@server:port/database?TrustServerCertificate=Yes"
        
  - name: sqlserver2
    type: Sqlserver
    connection:
      settings:
        # You can also specify the connection string as a dictionary
        host: "host"
        port: "1234"
        username: "user"
        password: "pass"
        database: "the-db"
```

### Connection String
The connection string is the generalized pyodbc/pymyssql connection string. You might have to append a driver specifier after your string like follows, depending on which drivers you have installed on your system. To determine which drivers are installed, you can use the following python scrript:

```python
import pyodbc

driver = str.replace(pyodbc.drivers()[0], " ", "+")
parameter = f"&driver={driver}"
print(f"{your_connection_string}{parameter}")

```

The driver is then appended to the string as follows:
```
mssql://user:password@server:port/database?TrustServerCertificate=Yes&driver=ODBC+Driver+17+for+SQL+Server
```

## Sync Settings
The settings for each sync are specified below. The `source` field should match the name of the source specified in the `sources` array.

### As a Source
To use SQL Server as a source to retrieve data from, you can either specify the "query" or the "table" field. If you specify the "query" field, the query will be used to retrieve data. If you specify the "table" field, the table will be used in a `SELECT * FROM <table>` query. 
```yaml
- source: "sqlserver"
  sourceConfig:
    table: "table"
    query: |
        SELECT * FROM table
    unique_columns:
      - id
    skip_columns:
      - street_address
```

### As a destination
When used as a destination, the "table" field has to be specified. You can still use the "query" field to fetch the data, but the "table" field will be used to insert, update and delete data.
```yaml
- source: "sqlserver"
  sourceConfig:
    table: "table"
    query: |
        SELECT * FROM table
    unique_columns:
      - id
    skip_columns:
      - street_address
```