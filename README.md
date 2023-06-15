<h1 style="width: 100%; text-align: center; margin-bottom: 20px; border-bottom: 0px;">BeETL: Extensible Python/Polars-based ETL Framework</h1>
<p style="text-align: center; margin-bottom: 30px;"><img src="./_static/BeETL.png" style="max-width: 400px;"><img src="./docs/_static/BeETL.png" style="max-width: 400px;"><br/></p>
BeETL was born from a job as Integration Developer where a majority of the integrations we develop follow the same pattern - get here, transform a little, put there (with the middle step frequently missing altogether). 

After building our 16th integration between the same two systems with another manual template, we decided to build BeETL. BeETL is currently limited to one datasource per source and destination per sync, but this will be expanded in the future. One configuration can contain multiple syncs.

Note: Even though most of the configuration below is in YAML format, you can also use JSON or a python dictionary.

## Todo:
- [ ] Soft Delete/Hard Delete

## TOC
- [Installation](#installation)
  - [From PyPi](#from-pypi)
  - [From Source](#from-source)
- [Quick Start](#quick-start)
- [Documentation](https://beetl.hoglan.dev/en/latest/)
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

## Quick Start
The following is the minimum amount of configuration needed to get started with a simple sync

```python
from src.beetl.beetl import Beetl, BeetlConfig

sync_config = {
    # The version of the config file, currently V1
    "version": "V1",
    
    # The datasources to move data between
    "sources": [
        {
            # The identifier for the datasource
            "name": "mysql_db",

            # The type (ex. Sqlserver, Rest, Itop)
            "type": "Mysql",

            # The connection settings for the datasource (connection string or host/user/password)
            "connection": {
              "settings": {
                "connection_string": "mysql://user:password@host:3306/database"
              }
            }
        },
        {
            "name": "postgres_db",
            "type": "Postgres",
            "connection": {
              "settings": {
                "connection_string": "postgresql://user:password@host:5432/database"
              }
            }
        }
    ],
    # The configuration for the sync(s) to run
    "sync": [
        {
            # The source and destination identifiers
            "source": "mysql_db",
            "destination": "postgres_db",

            # The configuration for source/destination
            "sourceConfig": {
                # The query with data to fetch
                "query": "SELECT field1, field2, field3 FROM table1",
                
                # The column descriptions for the query
                "columns": [
                    {
                        # The name of the column/field
                        "name": "field1",

                        # The data type
                        "type": "Int32",

                        # Whether the column is considered unique
                        # (unique cols will be used for comparison)
                        "unique": True
                    },
                    {
                        "name": "field2",
                        "type": "Utf8",
                        "unique": False
                    },
                    {
                        "name": "field3",
                        "type": "Utf8",
                        "unique": False
                    }
                ]
            },
            "destinationConfig": {
                # The table to insert data into
                "table": "table1",

                # The columns to insert data into
                "columns": [
                    {
                        # The name of the column/field
                        "name": "field1",

                        # The data type
                        "type": "Int32",

                        # Whether the column is considered unique
                        # (unique cols will be used for comparison)
                        "unique": True
                    },
                    {
                        "name": "field2",
                        "type": "Utf8",
                        "unique": False
                    },
                    {
                        "name": "field3",
                        "type": "Utf8",
                        "unique": False,
                        
                        # Will be created on insert, but not updated
                        "skip_update": True
                    }
                ]
            },
            "sourceTransformers": {},
            "insertionTransformers": {}
        }
    ]
}
```

### Secrets from Environment Variables
In case you want to save your secrets in environment variables instead of in the yaml configuration file, you can save them as a json object to an environment variable and replace the "sources"-section with sourcesFromEnv setting.

Note that the "sources" and "sourcesFromEnv" options are mutually exclusive.

```python
sync_config = {
    # The version of the config file, currently V1
    "version": "V1",

    # Fetch source configuration from environment variable BEETL_SOURCES
    "sourcesFromEnv": "BEETL_SOURCES",

    # The datasources to move data between
    "sync": [
        .....
```

```yaml
version: "V1"
sourcesFromEnv: "BEETL_SOURCES"
sync:
  - ......
```

```json
{
    "version": "V1",
    "sourcesFromEnv": "BEETL_SOURCES",
    "sync": [
        ......
```

The format of the sources configuration is the same as the one normally under the "sources"-section:

```python
[
    {
        # The identifier for the datasource
        "name": "mysql_db",

        # The type (ex. Sqlserver, Rest, Itop)
        "type": "Mysql",

        # The connection settings for the datasource (connection string or host/user/password)
        "connection": {
            "settings": {
            "connection_string": "mysql://user:password@host:3306/database"
            }
        }
    },
    {
        "name": "postgres_db",
        "type": "Postgres",
        "connection": {
            "settings": {
            "connection_string": "postgresql://user:password@host:5432/database"
            }
        }
    }
]
```