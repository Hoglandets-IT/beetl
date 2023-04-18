# BeETL - Extensible python-based engine for ETL tasks
BeETL was born from a job as Integration Developer where a majority of the integrations we develop follow the same pattern - get here, transform a little, put there (with the middle step frequently missing altogether). After building our 16th integration between the same two systems with another manual template, we decided to build BeETL.
Currently limited to one source and one destination(source) per sync, but this will be expanded in the future.
Note: Even though all the configuration below is in YAML format, you can also use JSON or a python dictionary.

## Concepts
You can find a more detailed explanation of the concepts below in the [documentation](https://beetl.readthedocs.io/en/latest/). The folder "tests" contains a number of examples of how to use the engine.

### Source(/Destination)
A source is a data-storage type that can be used to retrieve or store data. This can be a file (XML/JSON/Excel/CSV/others), API, Database or any other data storage type.
A basic source consists of the following configuration:
```yaml
# The source identifier, used for tying a source to a sync
- name: "src-name"
  # Type of source, identifies a source class. See built-in sources below.
  type: "static"
  # Configuration for the source, see built-in sources below
  config:
    # Column configuration
    columns:
      # Column ID/NAme
      - name: "id"
        # Data type (polars, see below)
        type: "Utf8"
        # Whether the column is unique
        unique: True
        # Whether to skip updating any changes to the destination column
        skip_update: True
```

### Sync
A sync consists of two sources, one where the data is retrieved and one where the data is inserted/updated/deleted.
The sync process consists of the following steps (Which are explained in greater detail in the "Development" section below):
- Extract data from the source<br>
  This queries the source and retrieves the fields specified in the configuration.
- Run any specified source transformers<br>
  See transformers below.
- Run any specified field transformers
  See transformers below.
- Extract data from the destination
  This queries the destination and retrieves the fields specified in the configuration.
- Compare the data to extract which rows to insert, update and delete
  This will run a comparison based on the columns marked as unique to determine insert/delete and all columns to determine update.
- Run the insert/update/delete queries on the destination
  This instructs the source module to handle insert/update/delete queries on the destination.

This is an example of a sync configuration:
```yaml
sync:
    # Specify the datasource where the data is fetched
  - source: "source_data"
    # Specify the datasource where the data will be updated
    destination: "destination"
    # Specify a source transformer to run on the data
    sourceTransformer: "transform.source"
    # Specify any number of field transformers to run on the data
    fieldTransformers:
      - transformer: "strings.lowercase"
        config:
          inField: "name"
          outField: "nameLowercase"
```

### Transformers
Transformers are drop-in functions to modify the data in any number of ways. There are a number of built-in transformers, but you can also insert your own at runtime.
There are two types of transformers

#### Field Transformers
When only needing smaller modifications to some of the fields, this can be done with field transformers. A field transformer is a python function that performs any number of standard operations on the dataset, for example:

```yaml
fieldTransformers:
    # Converts the given field to lowercase, either in-place or to a new field
    # Except when being overwritten, the original fields are always 
    # preserved unless manually dropped
  - transformer: "strings.lowercase"
    config:
      inField: "name"
      outField: "nameLowercase"

    # Convert the given field to uppercase
  - transformer: "strings.uppercase"
    config:
      inField: "name"
      outField: "nameUppercase"

    # Joins multiple strings to a single (new or existing) column
  - transformer: "strings.join"
    config:
      inFields:
        - "nameLowercase"
        - "nameUppercase"
      outField: "nameVariations"
      separator: "/"

    # Split the email string to get username and domain
  - transformer: "string.split"
    config:
      inField: "email"
      outFields:
        - "username"
        - "domain"
      separator: "@"

    # Drop the temporary columns for nameVariations
  - transformer: "frames.drop_columns"
    config:
      columns:
        - "nameLowercase"
        - "nameUppercase"
```

#### Source Transformers
When handling large or complex datasets, it may be necessary to transform the data in a way that makes it impractical to do with field transformers. A source transformer is a python function that takes in the entire dataset, and returns the transformed dataset.
The source transformer is specified in the sync configuration:
```yaml
sync:
  - source: "source_data"
    destination: "destination"
    sourceTransformer: "transform.source"
```

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
