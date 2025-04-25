# Getting Started

This page demonstrates a couple of quick usage examples for BeETL together with how the flow happens

## Installation

### From PyPi
```bash
#/bin/bash
python -m pip install beetl
```

### From Source
```bash
#/bin/bash
# Clone and enter the repository
git clone https://github.com/Hoglandets-IT/beetl.git
cd ./beetl
# Install the build tools
python -m pip install build
# Build beetl
python -m build
# Install beetl from locally built package
python -m pip install ./dist/*.tar.gz
```

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
sync:
  - source: csv
    sourceConfig:
      file:
        path: /path/to/users.csv
        separator: ","
        qualifier: '"'
    destination: sqlserver
    destinationConfig:
      table: users
      query: |
        SELECT id, name, age, email, phone FROM [DATABASE].dbo.[users]
      unique_columns:
        - id
    comparisonColumns:
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
      # The csv source will fetch all the columns from the source
      # For other sources you can provide what columns to fetch
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
    comparisonColumns:
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

### Destination Transformers
Destination transformers work in the same way as source transformers, and can transform fields from the destination before comparison.
This could be used in instances where you, for example, want to compare the data in a case-insensitive way by transforming both identifiers to upper- or lowercase.

You could then via the insertion transformers transform the name_backup column back to the name column.

::: warning
If you use transformers that require the original data, for example the iTop relations transformer under insertions, make sure that you restore the fields to their original state before running that transformer, like in the example below.

```yaml
    sourceTransformers:
      - transformer: strings.copy
        config:
          inField: name
          outField: name_backup
      - transformer: strings.upper
        config:
          inField: name
          outField: name

    destinationTransformers:
      - transformer: strings.upper
        config:
          inField: name
          outField: name
    
    insertionTransformers:
      - transformer: strings.copy
        config:
          inField: name_backup
          outField: name
      
      - transformer: itop.relations
        config:
          field_relations:
            - source_field: org_id
              source_comparison_field: name
              target_class: Organization
              target_comparison_field: name
              use_like_operator: False

```
:::

### Insertion/Deletion Transformers

This basic flow has no insertion transformers defined, since they are a more advanced feature. You can read more about them in the  [transformers documentation](/transformers/using-transformers.html).

::: tip

**Under the hood**

BeETL will run the insertion/deletion transformers when the changes have been determined, and the data is ready to be inserted/deleted.

:::

### Data Insertion, Updates and Deletes

When the transformation is finished, the insert, update and delete steps will be run according to the configuration for the source/destination pair.



### Running this sync

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
### Diff tracking 
It is possible to configure beetl to store diffs for each sync in a source.
Check out the [diff functionality page](/diff.html) to see how it works.

### Dry run

While developing your integration it might be helpful to be able to see what is going to happen without applying any changes to your destination. You can do this by simply passing `dry_run=True` to the `sync` method like this.

```python
from beetl.beetl import Beetl

sync = Beetl.from_yaml("config.yaml")
results = sync.sync(dry_run=True)

```

The results is a list of ComparisonResult with the following schema

```python
{
  "create": polars.DataFrame
  "update": polars.DataFrame,
  "delete": polars.DataFrame,
}
```

If you print any of the dataframes you will be presented by an ascii table representation of what will be created, updated and deleted from the destination dataset.

### Generate update diffs (debug only)

As a tool to help you understand what is going to be updated you can tell beetl to perform a dry run and output a diff for each row that is going to be updated containing only the unique indentifiers and the values that have changed. You can do this by passing `generate_update_diff=True` to the `sync` method.

```python
from beetl.beetl import Beetl

sync = Beetl.from_yaml("config.yaml")
results = sync.sync(generate_update_diff=True)

# prints all diffs for sync 1
print(results[0])
```
