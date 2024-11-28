def to_mongodb_with_object_id_as_identifier(mysql_connection_string: str, mongodb_connection_string: str):
    if (not mongodb_connection_string or not mysql_connection_string):
        raise Exception("Connection string is required")

    return {
        "version": "V1",
        "sources": [
            {
                "name": "srcdb",
                "type": "Mysql",
                "connection": {
                    "settings": {
                        "connection_string": mysql_connection_string,
                    }

                }
            },
            {
                "name": "dstdb",
                "type": "Mongodb",
                "connection": {
                    "settings": {
                        "connection_string": mongodb_connection_string,
                        "database": "test",
                    }

                }
            }
        ],
        "sync": [
            {
                "source": "srcdb",
                "destination": "dstdb",
                "sourceConfig": {
                    "table": "srctable",
                    "query": "SELECT * FROM srctable",
                    "columns": [
                        {
                            "name": "id",
                            "type": "Int32",
                            "unique": True,
                        },
                        {
                            "name": "email",
                            "type": "Utf8",
                            "unique": False,
                        }
                    ]
                },
                "destinationConfig": {
                    "collection": "dstcollection",
                    "unique_columns": ["id"],
                    "comparison_columns": ["id", "email"],
                    "columns": [
                        {
                            "name": "id",
                            "type": "Int32",
                            "unique": True,
                        },
                        {
                            "name": "email",
                            "type": "Utf8",
                            "unique": False,
                        }
                    ]
                },
                "sourceTransformers": [
                    {
                        "transformer": "int.to_int64",
                        "config": {
                            "inField": "id"
                        }
                    }
                ],
                "destinationTransformers": [
                    {
                        "transformer": "frames.rename_columns",
                        "config": {
                            "columns": [
                                {"from": "_id", "to": "id"}
                            ]
                        }
                    },
                    {
                        "transformer": "structs.jsonpath",
                        "inField": "address",
                        "outField": "city",
                        "config": {
                            "jsonPath": "$.city"
                        }
                    },
                    {
                        "transformer": "structs.jsonpath",
                        "inField": "children",
                        "outField": "children",
                        "config": {
                            "jsonPath": "$.*.name"
                        }
                    },
                    {
                        "transformer": "strings.join_listfield",
                        "config": {
                            "inField": "children",
                            "outField": "children",
                            "separator": ","
                        }
                    },
                    {
                        "transformer": "frames.project_columns",
                        "config": {
                            "columns": [
                                ["id", "name", "email", "city", "children"]
                            ]
                        }
                    }
                ],
                "insertionTransformers": [
                    {
                        "transformer": "frames.rename_columns",
                        "config": {
                            "columns": [
                                {"from": "id", "to": "_id"}
                            ]
                        }
                    },
                    {
                        "transformer": "strings.split_into_listfield",
                        "config": {
                            "separator": ",",
                            "inField": "children",
                        }
                    },
                    {
                        "transformer": "structs.compose_list_of_struct",
                        "config": {
                            "map": {"name": "children"},
                            "outField": "address"
                        }
                    },
                    {
                        "transformer": "structs.compose_struct",
                        "config": {
                            "map": {"city": "city"},
                            "outField": "address"
                        }
                    },
                    {
                        "transformer": "frames.project_columns",
                        "config": {
                            "columns": ["_id", "name", "email", "address", "first_child", "second_child"]
                        }
                    }
                ]
            }
        ]
    }
