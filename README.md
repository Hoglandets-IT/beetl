<p style="text-align: center"><img src="./_static/BeETL.png" style="max-width: 400px;"><br/>
</p>
<h1 style="width: 100%; text-align: center; margin-bottom: 30px; border-bottom: 0px;">Extensible Python/Polars-based ETL Framework</h1>

BeETL was born from a job as Integration Developer where a majority of the integrations we develop follow the same pattern - get here, transform a little, put there (with the middle step frequently missing altogether). 

After building our 16th integration between the same two systems with another manual template, we decided to build BeETL. BeETL is currently limited to one datasource per source and destination per sync, but this will be expanded in the future. One configuration can contain multiple syncs.

Note: Even though most of the configuration below is in YAML format, you can also use JSON or a python dictionary.

## TOC
- [Installation](#installation)
  - [From PyPi](#from-pypi)
  - [From Source](#from-source)
- [Concepts](#concepts)
  - [Datasources](#datasources)
  - [Sync](#sync)
  - [Transformers](#transformers)
    - [Field Transformers](#field-transformers)
    - [Source Transformers](#source-transformers)
- [Documentation](https://beetl.readthedocs.io/en/latest/)
- [Source Code](https://github.com/hoglandets-it/beetl)

## Installation
### From PyPi
```bash
pip3 install beetl
```

### From Source
```bash
git clone https://
python3 setup.py install
```


## Concepts
The engine is based largely on the Polars library, meaning the comparison and transformations are done in-memory and very quickly. Numbers show that Polars can outperform Pandas by a factor of 10-100x, depending on the operation.

You can find a more detailed explanation of the concepts below in the [documentation](https://beetl.readthedocs.io/en/latest/). The folder "tests" contains a number of examples of how to use the engine.

### Datasources
A datasource is a data-storage type that can be used to retrieve or store data. This can be a file (XML/JSON/Excel/CSV/others), API, Database or any other data storage type.
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