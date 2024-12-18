def to_mongodb_with_object_id_as_identifier(connectionString: str):
    if not connectionString:
        raise Exception("Connection string is required")

    return {
        "version": "V1",
        "sources": [
            {
                "name": "database",
                "type": "Mongodb",
                "connection": {
                    "settings": {
                        "connection_string": connectionString,
                        "database": "test",
                    }
                },
            }
        ],
        "sync": [
            {
                "source": "database",
                "destination": "database",
                "sourceConfig": {
                    "collection": "src",
                },
                "destinationConfig": {"collection": "dst", "uniqueFields": ["_id"]},
                "comparisonColumns": [
                    {"name": "_id", "type": "Utf8", "unique": True},
                    {
                        "name": "name",
                        "type": "Utf8",
                    },
                    {
                        "name": "email",
                        "type": "Utf8",
                    },
                    {
                        "name": "address",
                        "type": "Struct",
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "strings.to_object_id",
                        "config": {"inField": "_id"},
                    }
                ],
                "deletionTransformers": [
                    {
                        "transformer": "strings.to_object_id",
                        "config": {"inField": "_id"},
                    }
                ],
            }
        ],
    }


def to_mongodb_with_int_as_identifier(connectionString: str):
    if not connectionString:
        raise Exception("Connection string is required")

    return {
        "version": "V1",
        "sources": [
            {
                "name": "database",
                "type": "Mongodb",
                "connection": {
                    "settings": {
                        "connection_string": connectionString,
                        "database": "test",
                    }
                },
            }
        ],
        "sync": [
            {
                "source": "database",
                "destination": "database",
                "sourceConfig": {
                    "collection": "src",
                },
                "destinationConfig": {"collection": "dst", "uniqueFields": ["_id"]},
                "comparisonColumns": [
                    {"name": "_id", "type": "Int64", "unique": True},
                    {
                        "name": "name",
                        "type": "Utf8",
                    },
                    {
                        "name": "email",
                        "type": "Utf8",
                    },
                    {
                        "name": "address",
                        "type": "Struct",
                    },
                ],
            }
        ],
    }


def to_mysql_with_object_id_as_identifier(
    mongodb_connection_string: str, mysql_connection_string: str
):
    if not mongodb_connection_string:
        raise Exception("Connection string is required")

    return {
        "version": "V1",
        "sources": [
            {
                "name": "srcdb",
                "type": "Mongodb",
                "connection": {
                    "settings": {
                        "connection_string": mongodb_connection_string,
                        "database": "test",
                    }
                },
            },
            {
                "name": "dstdb",
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
                "source": "srcdb",
                "destination": "dstdb",
                "sourceConfig": {
                    "collection": "src",
                },
                "destinationConfig": {"table": "dst", "uniqueColumns": ["id"]},
                "comparisonColumns": [
                    {"name": "id", "type": "Utf8", "unique": True},
                    {
                        "name": "name",
                        "type": "Utf8",
                    },
                    {
                        "name": "email",
                        "type": "Utf8",
                    },
                ],
                "sourceTransformers": [
                    {
                        "transformer": "frames.rename_columns",
                        "config": {"columns": [{"from": "_id", "to": "id"}]},
                    },
                    {
                        "transformer": "structs.jsonpath",
                        "config": {
                            "jsonPath": "$.*.name",
                            "inField": "children",
                            "outField": "children_concatinated",
                        },
                    },
                    {
                        "transformer": "frames.drop_columns",
                        "config": {"columns": ["children"]},
                    },
                    {
                        "transformer": "strings.join_listfield",
                        "config": {
                            "inField": "children_concatinated",
                            "outField": "children",
                            "separator": ", ",
                        },
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
                        "transformer": "frames.project_columns",
                        "config": {
                            "columns": ["id", "name", "email", "city", "children"]
                        },
                    },
                ],
            }
        ],
    }
