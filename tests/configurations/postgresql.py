def to_postgres(connectionString: str):
    if not connectionString:
        raise Exception("Connection string is required")

    return {
        "version": "V1",
        "sources": [
            {
                "name": "database",
                "type": "Postgresql",
                "connection": {"settings": {"connection_string": connectionString}},
            }
        ],
        "sync": [
            {
                "source": "database",
                "destination": "database",
                "sourceConfig": {
                    "table": "srctable",
                },
                "destinationConfig": {
                    "table": "dsttable",
                    "uniqueColumns": ["id"],
                },
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
                    },
                ],
            }
        ],
    }


def diff_to_postgres(connectionString: str):
    if not connectionString:
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
                "type": "Postgresql",
                "connection": {"settings": {"connection_string": connectionString}},
            },
        ],
        "sync": [
            {
                "name": "sync",
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
                        "type": "Postgresql",
                        "name": "diff",
                        "config": {
                            "table": "diff",
                        },
                    }
                },
            }
        ],
    }
