# Python Synchronization Engine based on Polars DataFrames

## Installation
### From PyPi
```bash
pip3 install beetl
```

### From Source
```bash
python3 setup.py install
```

## External Dependencies
### SQL Server Connections
```bash
# Debian Example
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev

# RedHat Example
curl https://packages.microsoft.com/config/rhel/8/prod.repo > /etc/yum.repos.d/mssql-release.repo
ACCEPT_EULA=Y yum install -y msodbcsql17 unixODBC-devel
```

### MySQL Connection
```bash
# Debian/Ubuntu
apt-get install -y libmysqlclient-dev

# Redhat
yum install -y mysql-devel
```

### PostgreSQL Connection
```bash
# Debian/Ubuntu
apt-get install -y libpq-dev

# Redhat
yum install -y postgresql-devel
```

### SQLite Connection
```bash
# Debian/Ubuntu
apt-get install -y libsqlite3-dev

# Redhat
yum install -y sqlite-devel
```

## How the engine works
The engine does its work in four stages:

### 1. Data Retrieval
The data is retrieved from the Source, which can be File-based, SQL-based or one of the other sources.
You can also use a custom source by implementing the SourceInterfaceConfig, SourceConnectionSettings and SourceInterface classes.

### 2. Data Transformation
The goal of this step is that the data that comes out is transformed and formatted in an identical way to the destination database

### 3. Get the existing data from the destination
Retrieve the data that is already in the destination

### 4. Compare the data
Run a function to compare the data by the unique keys, and determine rows that need to be inserted, updated and deleted

### 5. Run Insert/Update/Delete queries
Run queries on the destination database/table to insert, update and delete data

## Configuration
This is the configuration file for the engine, and can be either YAML, JSON or a python dictionary.

```yaml
# This determines the version of the configuration file
# for backwards compatibility.
configVersion: "1"




## General Configuration
```yaml
---
# Configuration for synchronizations in yaml
configVersion: 1
sync:
    # The datasource to retrieve data from
  - source:
      type: relational
      config:
        connectionString: "mysql://user:pass@server/Database"
      query: "SELECT * FROM table"
      model:
        id: int
        name: string
        age: int
    format:
      # No transformation done
      id: {}
      name: {}
      
      # Split and choose first in list
      firstName:
        type: string
        source: name
        transform:
          - action: split
            args:
              char: " "
          - action: index
            args:
              index: 0
      # Split and choose last in list
      lastName:
        type: string
        source: name
        transform:
          - action: split
            args:
              - " "
          - action: index
            args:
              - "-1"
      
      # Generate an uuid
      uid:
        type: string
        source: false
        generate:
          type: uuid
      
      # Generate a random street address
      street:
        type: string
        source: false
        generate:
          type: faker
          args:
            - street_address

      # Use a custom function for formatting
      corporateId:
        type: string
        source: name,uuid
        custom:
          - class: CustomClass
            function: generateCorporateId
    
    # Where to put the data
    destination:
        type: relational
        config:
          connectionString: "mysql://user:pass@server/Database"
        table: "table"
        # Whether to insert, update, delete or all
        modes:
          insert: true
          update: true
          delete: true
        # Whether to use soft delete, set to false if you want to delete the rows
        softDelete:
            enabled: true
            field: deleted
            value: true
        fieldMapping:
          id: id
          name: name
          firstName: firstName
          lastName: lastName
          uid: uid
          street: street
          corporateId: corporateId
        uniqueKeys:
          - id
        preventUpdate:
          - corporateId
```

## Data Sources
### SQL/Relational Databases
```yaml
...
source:
  type: relational
  config:
    connectionString: "mysql://user:pass@server/Database"
  query: "SELECT * FROM table"
...
```
### Files
```yaml
...
source:
  type: file
  config:
    path: "path/to/file"
    charset: utf-8
    format: json
...
```

### Urls
```yaml
...
source:
  type: url
  config:
    url: "https://example.com/file.json"
    type: get (post/patch/delete...)
    basicAuth: false
    headers: {}
    authConfig:
      username: "username"
      password: "password"
...
authType: basic
authConfig:
    username: "username"
    password: "password"
...
authType: header
authConfig:
    header-1: "content"
    header-2: "content"
...
authType: certificate
authConfig:
    path: "path/to/certificate"
    key: "keyphrase"
...
authType: none
```
