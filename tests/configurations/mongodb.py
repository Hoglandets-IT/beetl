def to_mongodb_with_object_id_as_identifier(connectionString: str):
    if (not connectionString):
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

                }
            }
        ],
        "sync": [
            {
                "source": "database",
                "destination": "database",
                "sourceConfig": {
                    "collection": "src",
                    "unique_columns": ["_id"],
                    "comparison_columns": ["_id", "email"],
                    "columns": [
                        {
                            "name": "_id",
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
                    "collection": "dst",
                    "unique_columns": ["_id"],
                    "comparison_columns": ["_id", "email"],
                    "columns": [
                        {
                            "name": "_id",
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
                "insertionTransformers": [
                    {
                        "transformer": "strings.to_object_id",
                        "config": {
                            "inField": "_id"
                        }
                    }
                ],
                "deletionTransformers": [
                    {
                        "transformer": "strings.to_object_id",
                        "config": {
                            "inField": "_id"
                        }
                    }
                ]
            }
        ]
    }


def to_mongodb_with_int_as_identifier(connectionString: str):
    if (not connectionString):
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

                }
            }
        ],
        "sync": [
            {
                "source": "database",
                "destination": "database",
                "sourceConfig": {
                    "collection": "src",
                    "unique_columns": ["_id"],
                    "comparison_columns": ["_id", "email"],
                    "columns": [
                        {
                            "name": "_id",
                            "type": "Int32",
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
                    "collection": "dst",
                    "unique_columns": ["_id"],
                    "comparison_columns": ["_id", "email"],
                    "columns": [
                        {
                            "name": "_id",
                            "type": "Int32",
                            "unique": True,
                        },
                        {
                            "name": "email",
                            "type": "Utf8",
                            "unique": False,
                        }
                    ]
                }
            }
        ]
    }
