<h1 style="width: 100%; text-align: center; margin-bottom: 20px; border-bottom: 0px;">BeETL: Extensible Python/Polars-based ETL Framework</h1>
<p style="text-align: center; margin-bottom: 30px;"><img src="./docs/images/beetl.jpg" style="max-width: 400px;" alt=" "><br/></p>
BeETL was born from a job as Integration Developer where a majority of the integrations we develop follow the same pattern - get here, transform a little, put there (with the middle step frequently missing altogether). 

After building our 16th integration between the same two systems with another manual template, we decided to build BeETL. BeETL is currently limited to one datasource per source and destination per sync, but this will be expanded in the future. One configuration can contain multiple syncs.

Note: Even though some of the configuration below is in YAML format, you can also use JSON or a python dictionary.

## TOC
- [Minimal example](#minimal-example)
- [Installation](#installation)
  - [From PyPi](#from-pypi)
  - [From Source](#from-source)
- [Getting Started](#getting-started)
- [Development Environment](#development-environment)
- [Documentation](https://beetl.docs.hoglan.dev/)
- [Change Notes](https://beetl.docs.hoglan.dev/getting-started/change-notes.html)
- [Source Code](https://github.com/hoglandets-it/beetl)

## Minimal example

```python
# Syncing users from one table to another in the same database
from src.beetl.beetl import Beetl, BeetlConfig
config = BeetlConfig({
    "version": "V1"
    "sources": [
        {
            "name": "Sqlserver",
            "type": "Sqlserver",
            "connection": {
                "settings": {
                    "connection_string": "Server=myServerAddress;Database=myDataBase;User Id=myUsername;Password=myPassword;"
                }
            }
        },
    "sync": [
        {
            "name": "Sync between two tables in a sql server",
            "source": "Sqlserver",
            "sourceConfig": {
                "query": "SELECT id, name, email FROM users"
            }
            "destination": "SqlServer",
            "destinationConfig": {
                "table": "users",
                "unique_columns": ["id"]
            }
            "comparisonColumns": [
                {
                    "name": "id",
                    "type": "Int32",
                    "unique": True
                },
                {
                    "name": "name",
                    "type": "Utf8"
                },
                {
                    "name": "email",
                    "type": "Utf8"
                }
            ]
        }
    ]
})

Beetl(config).sync()

```

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

## Getting Started

All the latest information about how to use beetl is located at the [official docs](https://beetl.docs.hoglan.dev/getting-started.html).


## Development Environment

The easiest way to get started is to use the included devcontainer. 

### Requirements
- Docker
- Visual Studio Code

### Steps

1. Clone the repository.
1. Open the repository in Visual Studio Code.
1. Install the recommended extensions.
1. Using the command palette (`ctrl+shift+p`) search for `reopen in container` and run it.
   - The devcontainer will now be provisioned in your local docker instance and vscode will automatically connect to it.
1. You can now use the included launch profiles to either open the docs or run the tests file.
1. You can also use the built-in test explorer to run the available test.