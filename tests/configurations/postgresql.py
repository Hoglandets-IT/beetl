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
                    "columns": [
                        {
                            "name": "id",
                            "type": "Int32",
                            "unique": True,
                            "skip_update": True
                        },
                        {
                            "name": "name",
                            "type": "Utf8",
                            "unique": False,
                            "skip_update": False
                        },
                        {
                            "name": "email",
                            "type": "Utf8",
                            "unique": False,
                            "skip_update": False
                        }
                    ]
                },
                "destinationConfig": {
                    "table": "dsttable",
                    "columns": [
                        {
                            "name": "id",
                            "type": "Int32",
                            "unique": True,
                            "skip_update": True
                        },
                        {
                            "name": "name",
                            "type": "Utf8",
                            "unique": False,
                            "skip_update": False
                        },
                        {
                            "name": "email",
                            "type": "Utf8",
                            "unique": False,
                            "skip_update": False
                        }
                    ]
                }
            }
        ]
    }
