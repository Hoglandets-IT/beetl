# Getting Started

This page demonstrates a couple of quick usage examples for BeETL together with how the flow happens

## Sources
To begin, choose your sources. In this example, we will use SQL Server and a CSV file.

### CSV Data

```csv
id,name,age,email,phone
1,John Doe,25,SMTP:john.doe@example.com,123-456-7890
2,Jane Doe,24,SMTP:jane.doe@example.com,123-456-7891
3,Jim Doe,23,SMTP:jim.doe@example.com,123-456-7892
```

### SQL Server Data
| id | name | age | email | phone |
|----|------|-----|-------|-------|
| 1  | John Doe | 25 | john.doe@example.com | +11234567890 |
| 2  | Jane Doe | 20 | jane.doe@example.com | +11234567891 |
| 4 | Jack Doe | 30 | jack.doe@example.com | +11234567892 |


## Configuration


In this example, we'll write the configuration as yaml. You can also use JSON, or write it directly in Python.

This is the complete configuration, we'll explain the different parts below.
```yaml
version: V1
sources:
  - name: csv
    type: File
    connection:
      content_type: text/csv
  - name: sqlserver
    type: Sqlserver
    connection:
      settings:
        connection_string: "Server=myServerAddress;Database=myDataBase;User Id=myUsername;Password=myPassword;"
        fast_executemany: False
sync:
  - source: csv
    sourceConfig:
      file:
        path: /path/to/users.csv
        separator: ","
        qualifier: '"'
      columns:
        - name: id
          type: Uint8
          unique: True
        - name: name
          type: Utf8
        - name: age
          type: Uint8
        - name: email
          type: Utf8
        - name: phone
          type: Utf8
    destination: sqlserver
    destinationConfig:
      table: users
      query: |
        SELECT id, name, age, email, phone FROM [DATABASE].dbo.[users]
      columns:
        - name: id
          type: Uint8
          unique: True
        - name: name
          type: Utf8
        - name: age
          type: Uint8
        - name: email
          type: Utf8
        - name: phone
          type: Utf8
    sourceTransformers:
      - transformer: strings.replace_all
        config:
          inField: email
          search: "SMTP:"
          replace: ""
      - transformer: regex.replace_all
        config:
          inField: phone
          pattern: "(\d{3})-(\d{3})-(\d{4})"
          replace: "+1$1$2$3"
```

### Sources
In the sources sections, you define which data sources you want to use. In this example, we have two sources: a CSV file and SQL Server Database.

Here, you only define the general settings for the source. Sync-specific settings (tables, queries) are defined in the inidividual pipelines.

::: tip

**Under the hood**

BeETL will load the configuration from the sources and do any initialization steps when the sources are loaded. This includes connecting to the sources to check whether they can be accessed.
:::

```yaml
sources:
  # The identifier for the source, used to reference it in the sync section
  - name: csv
    # The source type
    type: File
    # Source-specific settings. More on this in the source-specific documentation
    connection:
      content_type: text/csv
  - name: sqlserver
    type: Sqlserver
    connection:
      settings:
        connection_string: "Server=myServerAddress;Database=myDataBase;User Id=myUsername;Password=myPassword;"
        fast_executemany: False
```

### Source/Destination
In the sync section, you define the data flow. You define which source to use, which destination to write to, and how to transform the data.

Here, you define how to fetch the data from the source and what fields to use

::: tip

**Under the hood**

Beetl connects to the source and destination and fetches all the data according to the configuration.
:::

```yaml
sync:
  # The source name
  - source: csv
    sourceConfig:
      # Source-specific settings. More on this in the source-specific documentation
      file:
        path: /path/to/users.csv
        separator: ","
        qualifier: '"'
      # The columns to use from the source, their data types and whether the column is unique
      # There should be a minimum of one unique column per dataset
      # These columns don't yet need to match the destination columns, as we can transform them later
      columns:
        - name: id
          type: Uint8
          unique: True
        - name: name
          type: Utf8
        - name: age
          type: Uint8
        - name: email
          type: Utf8
        - name: phone
          type: Utf8
    # The destination name
    destination: sqlserver
    destinationConfig:
      # Destination-specific settings. More on this in the destination-specific documentation
      table: users
      query: |
        SELECT id, name, age, email, phone FROM [DATABASE].dbo.[users]
      # The columns to use from the destination, their data types and whether the column is unique
      # There should be a minimum of one unique column per dataset
      # These columns don't yet need to match the source columns, as we can transform them later
      columns:
        - name: id
          type: Uint8
          unique: True
        - name: name
          type: Utf8
        - name: age
          type: Uint8
        - name: email
          type: Utf8
        - name: phone
          type: Utf8
```

### Source Transformers
These transformers take one, or several, columns at a time and transform data directly in the source dataset. This is useful for cleaning up, combining or transforming data before comparison

In this case, we're removing the "SMTP:" prefix from the email column and replacing the phone number format with a standard format.

::: tip

**Under the hood**

BeETL will first apply all the source transformers, then run the data through a comparison algorithm to determine what has changed from the source to the destination

:::

```yaml
    sourceTransformers:
      # The transformer type
      - transformer: strings.replace_all
        # The transformer-specific settings. More on this in the transformer-specific documentation
        config:
          inField: email
          search: "SMTP:"
          replace: ""
      - transformer: regex.replace_all
        config:
          inField: phone
          pattern: "(\d{3})-(\d{3})-(\d{4})"
          replace: "+1$1$2$3"
```

### Insertion Transformers

This basic flow has no insertion transformers defined, since they are a more advanced feature. You can read more about them in the [insertion transformers documentation](/insertion-transformers)

::: tip

**Under the hood**

BeETL will run the insertion transformers when the changes have been determined, and the data is ready to be inserted

:::

### Data Insertion
To run the sync, you can either use the CLI or the Python API. Here's an example of how to run the sync using the Python API.

```python
from beetl.beetl import Beetl

sync = Beetl.from_yaml("config.yaml")
sync.sync()

```
The output from this sync will be the following:

```bash
$ python3 sync.py

Finished data retrieval from source: 0.93233
Finished data retrieval from destination: 1.06051
Starting source data transformation: 0.00013
Finished data transformation before comparison: 0.01157
Starting comparison: 0.00012
Successfully extracted operations from dataset: 6.97218
Insert: 1, Update: 1, Delete: 1
Starting database operations: 0.00024
Finished inserts, starting updates: 4.88824
Finished updates, starting deletes: 0.00046
Finished deletes, sync finished: 0.00019
Inserted: 1
Updated: 1
Deleted: 1

```
