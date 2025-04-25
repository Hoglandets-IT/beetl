"""Configs for testing the sqlserver source"""


def to_sqlserver(sqlserver_connection_string: str):
    """Config for testing that data can be synced into a sqlserver database."""
    if not sqlserver_connection_string:
        raise ValueError("Connection string is required")

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
    """Config for testing that sqlserver source can store diffs into a sqlserver instance."""
    if not sqlserver_connection_string:
        raise ValueError("Connection string is required")

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
                    "destination": {
                        "type": "Sqlserver",
                        "name": "dst",
                        "config": {"table": "diffs"},
                    }
                },
            }
        ],
    }


def to_sqlserver_with_replace_empty_strings(sqlserver_connection_string: str):
    """Config for testing that sqlserver replaces empty strings with null value if configured with the replace_empty_strings flag."""
    if not sqlserver_connection_string:
        raise ValueError("Connection string is required")

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
