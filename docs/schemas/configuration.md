# Beetl Configuration
The configuration is how to instruct beetl what to do and you can provide it in three ways.

**Python dictionary**
```python
from beetl.beetl import Beetl, BeetlConfig
config = BeetlConfig({
  "version": "V1",
  "sources": [],
  "sync": []
})

Beetl(config).sync()
```
**From yaml**
```python
from beetl.beetl import Beetl, BeetlConfig

config = BeetlConfig.from_json_file("config.json")

Beetl(config).sync()
```

**From yaml**
```python
from beetl.beetl import Beetl, BeetlConfig

config = BeetlConfig.from_yaml_file("config.yaml")

Beetl(config).sync()
```

## Schema


```yaml
# Config version
# Valid options: "V1"
version: V1
# Data sources connections
# List of SourceConfiguration from any of the sources defined under the Sources section in the docs
sources:
    # name of the source, can be whatever
  - name: sqlserver
    # type of data source, can be found in the docs for each source
    type: Sqlserver
    # source specific connection config, can be found in the docs for each source
    connection: <source specific connection config>
# alternative definition of sources taken from environment variables
# environment variable value must be in json format, see the section about this below
sourcesFromEnv: <environment-variable-name>
# Sync definitions
# A list of syncs that will be performed using the sources above
sync:
    # name of the sync
  - name: sync 1
    # name of the data source from the sources section
    source: sqlserver
    # source specific sync config, can be found in the docs for each source
    sourceConfig: <source specific sync config>
    # name of the data destination from the sources section
    destination: sqlserver
    # destination specific sync config, can be found in the docs for each source
    destinationConfig: <destination specific sync config>
    # definition of how data source and destination will be compared
    # list of ColumnSpecification, see the link below this example.
    comparisonColumns: []
    # definition of transformations that will be made to the source data prior to comparing
    # list of transformers, see the link below this example. 
    sourceTransformers: []
    # definition of transformations that will be made to the destination data prior to comparing
    # list of transformers, see the link below this example. 
    destinationTransformers: []
    # definition of transformations that will be made to the data that will be inserted and updated in the destination
    # list of transformers, see the link below this example. 
    insertionTransformers: []
    # definition of transformations that will be made to the data that will be deleted from the destination
    # list of transformers, see the link below this example. 
    deletionTransformers: []


```
Related schemas and docs
- [ColumnSpecification](/getting-started/columns.html)
- [Using Transformers](/transformers/using-transformers.html)

### Sources from Environment Variable
In case you want to save your secrets for your sources in environment variables instead of in the yaml configuration file, you can save them as a json object to an environment variable and replace the "sources"-section with sourcesFromEnv setting.

Note that the "sources" and "sourcesFromEnv" options are mutually exclusive.

