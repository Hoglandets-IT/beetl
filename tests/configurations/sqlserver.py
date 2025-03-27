def to_sqlserver(sqlserver_connection_string: str):
    if not sqlserver_connection_string:
        raise Exception("Connection string is required")

    return {
        "version": "V1",
        "sources": [
            {
                "name": "database",
                "type": "Sqlserver",
                "connection": {
                    "settings": {
                        "connection_string": sqlserver_connection_string,
                    }
                },
            },
        ],
        "sync": [
            {
                "source": "database",
                "destination": "database",
                "sourceConfig": {
                    "table": "src",
                },
                "destinationConfig": {
                    "table": "dst",
                    "uniqueColumns": ["id"],
                },
                "comparisonColumns": [
                    {
                        "name": "id",
                        "type": "Int64",
                        "unique": True,
                    },
                    {
                        "name": "email",
                        "type": "Utf8",
                    },
                    {
                        "name": "name",
                        "type": "Utf8",
                    },
                ],
            }
        ],
    }


def from_static_to_sqlserver_with_diff(sqlserver_connection_string: str):
    if not sqlserver_connection_string:
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
                        {"id": 5, "name": "test5", "age": 20},
                    ]
                },
            },
            {
                "name": "dst",
                "type": "Sqlserver",
                "connection": {
                    "settings": {
                        "connection_string": sqlserver_connection_string,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "diff_test",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "table": "dst",
                    "uniqueColumns": ["id"],
                },
                "comparisonColumns": [
                    {
                        "name": "id",
                        "type": "Int64",
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
                    "source_type": "Sqlserver",
                    "name": "dst",
                    "config": {"table": "diffs"},
                },
            }
        ],
    }
