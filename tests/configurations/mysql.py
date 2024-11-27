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
                    "table": "dst",
                    "columns": [
                        {
                            "name": "id",
                            "type": "Utf8",
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
                    "collection": "src",
                    "unique_columns": ["_id"],
                    "comparison_columns": ["_id", "email"],
                    "columns": [
                        {
                            "name": "id",
                            "type": "Utf8",
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
                    # TODO: transform dest
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
                        "config": {
                            "jsonPath": "$.*.name",
                            "inField": "children",
                            "outField": "children_concatinated",
                        }
                    },
                    {
                        "transformer": "frames.drop_columns",
                        "config": {
                            "columns": ["children"]
                        }
                    },
                    {
                        "transformer": "strings.join_listfield",
                        "config": {
                            "inField": "children_concatinated",
                            "outField": "children",
                            "separator": ", "
                        }
                    },
                    {
                        "transformer": "structs.jsonpath",
                        "config": {
                            "jsonPath": "$.city",
                            "inField": "address",
                            "outField": "city",
                        }
                    },
                    {
                        "transformer": "frames.project_columns",
                        "config": {
                            "columns": ["id", "name", "email", "city", "children"]
                        }
                    }
                ]
            }
        ]
    }
