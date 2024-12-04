def to_postgres(connectionString: str):
    if (not connectionString):
        raise Exception("Connection string is required")

    return {
        "version": "V1",
        "sources": [
            {
                "name": "database",
                "type": "Postgresql",
                "connection": {
                    "settings": {
                        "connection_string": connectionString
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
                    }
                ]
            }
        ]
    }
