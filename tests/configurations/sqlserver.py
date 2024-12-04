
def to_sqlserver(sqlserver_connection_string: str):
    if (not sqlserver_connection_string):
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

                }
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
                        "type": "Int32",
                        "unique": True,
                    },
                    {
                        "name": "email",
                        "type": "Utf8",
                    },
                    {
                        "name": "name",
                        "type": "Utf8",
                    }
                ]
            }
        ]
    }
