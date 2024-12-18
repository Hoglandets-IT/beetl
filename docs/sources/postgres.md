# PostgreSQL
The PostgreSQL source will load and save data to PostgreSQL servers.

- Type identifier: `Postgresql`

## Source Configuration 
Declare how the source should be connecting to the database instance.
```yaml
sources:
  - name: postgres_database_1
    type: Postgresql
    connection:
      settings:
        # connection_string, <string>, (optional*)
        # Mandatory if [host, port, username, password, database] aren't specified.
        # If provided it takes precedence over [host, port, username, password, database]
        connection_string: "postgresql://[username]:[password]@[host]:[port]/[database]"
        # host: <string>, (optional)
        # Can be ip-address or hostname
        host: "localhost"
        # port: <string>, (optional)
        port: "1234"
        # username: <string>, (optional)
        username: "admin"
        # password: <string>, (optional)
        password: "password"
        # database: <string>, (optional)
        # The name of the database within the database server
        database: "test"
```

## Sync Settings
Declare how the data should be queried and the schema for comparing the source with the destination.
```yaml
sync:
  - source: postgres_database_1
    destination: postgres_database_1
    sourceConfig:
      # query, <string>, (optional)
      # default is SELECT * from <table>
      query: SELECT * FROM source-table-name
      # table, <string>, (optional*)
      # Mandatory if query isn't specified
      table: source-table-name
      # unique_columns, <list<string>> (mandatory)
      # defines uniqueness of a row
      # used when modifying data in the source
      unique_columns:
        - id
      # skip_columns, <list<string>> (mandatory)
      # specified column names will not be inserted or updated in the source
      skip_columns:
        - street_address

    # The postgresql configs are the same regardless of direction of the sync
    destinationConfig:
      query: SELECT * FROM destination-table-name
      table: destination-table-name
      unique_columns:
        - id
```

## Example

### From PostgreSQL to PostgreSQL
This example syncs data from table `people` in database 1 to table `people` in database 2.

No transformations are done, the database schema is being kept.

#### What will happen?
**People of database 1**

|id|name|email|
|-|-|-|
|1|John Doe|johndoe@example.com|
|2|Jane Doe|janedoe@example.com|
|3|Jim Doe|jimdoe@example.com|

**People of database 2**

|id|name|email|
|-|-|-|
|1|John Doe|johndoe@example.com|
|2|Jane Doe|janedoe2@example.com|
|4|July Doe|julydoe@example.com|

**Result**
- Id will be used to uniquely identify each row across the databases.
- Name and email will be compared to see if a row needs updating or not.
- All rows in in database 1 not present in database 2 will be created.
- All rows in database 2 not present in database 1 will be deleted.


**People of database 2 after sync**

|id|name|email|
|-|-|-|
|1|John Doe|johndoe@example.com|
|2|Jane Doe|janedoe@example.com|
|3|Jim Doe|jimdoe@example.com|

- Jane Doe's email was updated.
- Jim Doe was created
- July Doe was dropped

#### Configuration

```yaml
sources:
  - name: database_1
    type: Postgresql
    connection:
      settings:
        connection_string: "postgresql://admin:password@localhost:1234/database_1"
  - name: database_2
    type: Postgresql
    connection:
      settings:
        host: "localhost"
        port: "1234"
        username: "admin"
        password: "password"
        database: "database_2"
sync:
  - source: database_1
    destination: database_2
    sourceConfig:
      query: SELECT id, name, email FROM people
      unique_columns: 
        - id
    destinationConfig:
      query: SELECT * FROM people
      unique_columns: 
        - id
    comparisonColumns:
      - name: id
        type: Int32
        unique: true
      - name: name
        type: Utf8
      - name: email
        type: Utf8
```
