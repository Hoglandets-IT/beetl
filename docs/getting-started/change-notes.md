# Change Notes

## 1.0.0

### New features ⭐
- Introducing deletion transformers.
   - If your destination requires a unique identifier to be in a specific format you can now perform tranformations on those fields.
- New supported data sources:
  - [MongoDB](/sources/mongodb.html)
  - [PostgreSQL](/sources/postgres.html)
- New Transformers
  - [frames.project_columns](/transformers/frames.html#project-columns)
  - [int.to_int64](/transformers/int.html#to-int64)
  - [string.split_into_listfield](/transformers/string.html#split-into-listfield)
  - [string.join_listfield](/transformers/string.html#join_listfield)
  - [string.to_object_id](/transformers/string.html#to_object_id)
  - [structs.compose_struct](/transformers/structs.html#compose-struct)
  - [structs.compose_list_of_structs](/transformers/structs.html#compose-list-of-structs)
- Updated Transformers
  - [structs.jsonpath](/transformers/structs.html#jsonpath) now supports the `*` selector.
- The sync method now returns a result object containing information about inserted, updated and deleted count.
- The documentation has been updated with:
  - This [changelog](/getting-started/change-notes.html).
  - A [flowchart](/getting-started/change-notes.html) explaining the beetl sync process.
  - And a lot of other additions from data sources to transformers and examples.


### Bugfixes 🐛
- The iTop source now properly warns when the soft_delete property is being used on a target class that does not support soft delete.

### Breaking changes ⛓️‍💥
- The `sourceConfig.columns` field has been deprecated and has been replaced by the mandatory `sync.comparisonColumns`. See the [documentation](/getting-started.html#configuration) and the section below about changes to sync columns.
- Previously the MySQL, SqlServer and PostgreSql sources built a query from the provided column specification if none was provided. That default has been changed into selecting all from the specified table `SELECT * FROM <table>`.
  - If you were using this automatic fallback functionality and don't want to fetch all columns from the source you will have to provide a query in the source configuration. See the [sqlserver documentation](/sources/sqlserver.html).
- Previously the iTop data source queried a combination of fields specified as the columns in the old deprecated `sourceConfig.columns` + the `custom_options.itop.comparison_field` fields specified in the columns. Since this property has been replaced with the sync level `comparisonColumns` you now have to specify what fields you want to query, and what columns you want to skip while updating. See the [iTop documentation](/sources/itop.html).
- The dataset of items that will be created or updated at the destination will now have the schema of the tranformed source dataset since this is where they originate from. Likewise the dataset of items that are going to be deleted from the destination will have the schema of the transformed destination since they don't necessarily have the same schema as the source and can't be transformed into the same schema automatically. This differs from the previous versions where the delete dataset only included unique columns.
- At compare time the transformed source and transformed destination will get all the comparison columns added to them as empty columns if they don't already exist in the dataset. After that the columns specified in the comparison columns will be cast to the type specified if they are of a castable type.

### Quality control 🔍
- Integration tests have been added for all implemented sources.
   - Tests are being conducted using the testcontainers library to run the sources and destination data sources as containers.
   - A mock restapi container was developed to support testing of the rest data source.
   - The iTop data source tests are the only tests that require an actual instance of the service. If you want to run them, open the tests.secrets.template.yaml and follow the instructions on how to provide the secrets needed.

### Changes to sync columns 🔃
With development on new document database sources it's becoming clear that the source configuration columns list is being used to define the data structure of both the source data and the data being compared. This is clearly not always the case since transformers can be applied to vastly change the structure between query and comparison.

For example you might want to fetch column A and B from the data source, use source transformers to transform the columns into C and D, drop column A and B and then compare only C and D with the destination. 

The schema of column A and B is only relevant at query and transformation, while C and D are only relevant at the time of comparison.

To accomodate the need of allowing the data in these two stages of the sync to look different we also need to change the configuration.

1. If a data source requires knowledge about what fields to fetch, what fields are concidered unique, which should be updated and which should be ignored the source configuration and documentation has been updated to reflect this.
1. The schema to aid the comparing process has been moved from `sync.sourceConfig.columns` and `sync.destinationConfig.columns` to `sync.comparisonColumns`. They have three valid properties `name`, `type` and `unique`, the last `custom_options` have been removed.
1. The `itop.relations` transformer that used the `custom_options` have been updated to take all necessary information as transformer options instead, eliminating the need to pass the whole configuration to any transformer.

#### Migation example
Over all you will notice less redundant column specification and information will be declared in the resource that needs it.

```python
old_config = {
        "version": "V1",
        "sources": [
            {
                "name": "database",
                "type": "Postgresql",
                "connection": {
                    "settings": {
                        "connection_string": "connection_abc"
                    }

                }
            }
        ],
        "sync": [
            {
                "source": "database",
                "destination": "database",
                "sourceConfig": {
                    "table": "srctable",
                    # columns will be removed
                    "columns": [
                        {
                            "name": "id",
                            "type": "Int32",
                            "unique": True,
                            "skip_update": True
                        },
                        {
                            "name": "name",
                            "type": "Utf8",
                            "unique": False,
                            "skip_update": False
                        },
                        {
                            "name": "email",
                            "type": "Utf8",
                            "unique": False,
                            "skip_update": False
                        }
                    ]
                },
                "destinationConfig": {
                    "table": "dsttable",
                    # columns will be removed
                    "columns": [
                        {
                            "name": "id",
                            "type": "Int32",
                            "unique": True,
                            "skip_update": True
                        },
                        {
                            "name": "name",
                            "type": "Utf8",
                            "unique": False,
                            "skip_update": False
                        },
                        {
                            "name": "email",
                            "type": "Utf8",
                            "unique": False,
                            "skip_update": True
                        }
                    ]
                }
            }
        ]
    }

migrated_config = {
        "version": "V1",
        "sources": [
            {
                "name": "database",
                "type": "Postgresql",
                "connection": {
                    "settings": {
                        "connection_string": "connection_abc"
                    }

                }
            }
        ],
        "sync": [
            {
                "source": "database",
                "destination": "database",
                "sourceConfig": {
                    "table": "srctable",
                    # query was added to restrict the columns fetched.
                    "query": "SELECT id, name, email FROM srctable"
                },
                "destinationConfig": {
                    "table": "dsttable",
                    # destination needs to know what columns to use as identifiers.
                    "unique_columns": ["id"]
                    # and what columns to skip inserting if any.
                    "skip_columns": ["email"]
                    # no query needed since the destination only contain the columns we want.

                },
                # comparison columns were moved here.
                # unique only needs to be set on unique columns.
                # skip_column has been movet to the destination config.
                "comparisonColumns": [
                        {
                            "name": "id",
                            "type": "Int32",
                            "unique": True,
                        },
                        {
                            "name": "name",
                            "type": "Utf8",
                        },
                        {
                            "name": "email",
                            "type": "Utf8",
                        }
                    ]
            }
        ]
    }
```

### For developers 🧑‍💻
- Dependencies updated
   - Polars version bumped from 0.17.5 to 1.14.0.
   - Pandas version was locked at 2.2.3.
