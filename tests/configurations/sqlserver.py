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


def to_sqlserver_with_replace_empty_strings(sqlserver_connection_string: str):
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
                    "replace_empty_strings": True,
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
                ],
            }
        ],
    }
