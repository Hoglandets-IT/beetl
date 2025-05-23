def to_mongodb_with_object_id_as_identifier(
    mysql_connection_string: str, mongodb_connection_string: str
):
    if not mongodb_connection_string or not mysql_connection_string:
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
                },
            },
            {
                "name": "dstdb",
                "type": "Mongodb",
                "connection": {
                    "settings": {
                        "connection_string": mongodb_connection_string,
                        "database": "test",
                    }
                },
            },
        ],
        "sync": [
            {
                "source": "srcdb",
                "destination": "dstdb",
                "sourceConfig": {
                    "table": "srctable",
                },
                "destinationConfig": {
                    "collection": "dstcollection",
                    "uniqueFields": ["_id"],
                },
                "comparisonColumns": [
                    {
                        "name": "id",
                        "type": "Int32",
                        "unique": True,
                    },
                    {
                        "name": "email",
                        "type": "Utf8",
                    },
                ],
                "sourceTransformers": [
                    {"transformer": "int.to_int64", "config": {"inField": "id"}}
                ],
                "destinationTransformers": [
                    {
                        "transformer": "frames.rename_columns",
                        "config": {"columns": [{"from": "_id", "to": "id"}]},
                    },
                    {
                        "transformer": "structs.jsonpath",
                        "config": {
                            "jsonPath": "$.city",
                            "inField": "address",
                            "outField": "city",
                        },
                    },
                    {
                        "transformer": "structs.jsonpath",
                        "config": {
                            "jsonPath": "$.*.name",
                            "inField": "children",
                            "outField": "children",
                        },
                    },
                    {
                        "transformer": "strings.join_listfield",
                        "config": {
                            "inField": "children",
                            "outField": "children",
                            "separator": ",",
                        },
                    },
                    {
                        "transformer": "frames.project_columns",
                        "config": {
                            "columns": ["id", "name", "email", "city", "children"]
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "frames.rename_columns",
                        "config": {"columns": [{"from": "id", "to": "_id"}]},
                    },
                    {
                        "transformer": "strings.split_into_listfield",
                        "config": {
                            "separator": ",",
                            "inField": "children",
                        },
                    },
                    {
                        "transformer": "structs.compose_list_of_structs",
                        "config": {"map": {"name": "children"}, "outField": "children"},
                    },
                    {
                        "transformer": "structs.compose_struct",
                        "config": {"map": {"city": "city"}, "outField": "address"},
                    },
                    {
                        "transformer": "frames.project_columns",
                        "config": {
                            "columns": ["_id", "name", "email", "address", "children"]
                        },
                    },
                ],
                "deletionTransformers": [
                    {
                        "transformer": "frames.rename_columns",
                        "config": {"columns": [{"from": "id", "to": "_id"}]},
                    },
                ],
            }
        ],
    }


def diff_to_mysql(mysql_connection_string: str):
    if not mysql_connection_string:
        raise Exception("Connection string is required")

    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {
                    "static": [
                        {"id": 2, "name": "test2", "age": 20},
                        {"id": 3, "name": "test3", "age": 20},
                        {"id": 4, "name": "test4", "age": 20},
                    ]
                },
            },
            {
                "name": "dst",
                "type": "Static",
                "connection": {
                    "static": [
                        {"id": 3, "name": "test", "age": 20},
                        {"id": 4, "name": "test4", "age": 20},
                        {"id": 5, "name": "test5", "age": 20},
                    ]
                },
            },
            {
                "name": "diff",
                "type": "Mysql",
                "connection": {
                    "settings": {
                        "connection_string": mysql_connection_string,
                    }
                },
            },
        ],
        "sync": [
            {
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {},
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
                        "name": "age",
                        "type": "Int64",
                    },
                ],
                "diff": {
                    "destination": {
                        "type": "Mysql",
                        "name": "diff",
                        "config": {
                            "table": "diff",
                        },
                    }
                },
            }
        ],
    }
