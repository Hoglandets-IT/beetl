from copy import deepcopy


def generate_from_static_to_static() -> dict:
    return deepcopy(_from_static_to_static)


_from_static_to_static = {
    "version": "V1",
    "sources": [
        {
            "name": "staticsrc",
                    "type": "Static",
                    "connection": {
                        "static": [
                            {
                                "id": 1,
                                "name": "John",
                                "email": "john@test.com"
                            },
                            {
                                "id": 2,
                                "name": "Jane",
                                "email": "jane@test.com"
                            },
                            {
                                "id": 3,
                                "name": "Steffen",
                                "email": "steffen@test.com"
                            }
                        ]
                    }
        },
        {
            "name": "staticdst",
                    "type": "Static",
                    "connection": {
                        "static": [
                            {
                                "id": 1,
                                "name": "John",
                                "email": "john@test.com"
                            },
                            {
                                "id": 4,
                                "name": "James",
                                "email": "jane@test.com"
                            },
                            {
                                "id": 3,
                                "name": "Stephen",
                                "email": "stephen@test.com"
                            }
                        ]
                    }
        }
    ],
    "sync": [
        {
            "source": "staticsrc",
            "destination": "staticdst",
            "sourceConfig": {
                "columns": [
                            {
                                "name": "id",
                                "type": "Utf8",
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
                "columns": [
                    {
                        "name": "id",
                                "type": "Utf8",
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
            "sourceTransformers": [],
            "destinationTransformers": []
        }
    ]
}


def generate_from_postgres_to_postgres(connectionString: str):
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


def generate_from_mongodb_to_mongodb_with_transformation(connectionString: str):
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
                    "projection": {
                        "_id": 1,
                        "id": 1,
                        "name": 1,
                        "address": 1
                    },
                    "filter": {
                        "id": {
                            "$gt": 1
                        }
                    },
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
                            "name": "city",
                            "type": "Utf8",
                            "unique": False,
                            "skip_update": False
                        },

                    ]
                },
                "destinationConfig": {
                    "collection": "dst",
                    "comparison_columns": ["id", "name", "email", "city"],
                    "columns": [
                        {
                            "name": "_id",
                            "type": "Binary",
                            "unique": False,
                            "skip_update": True
                        },
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
                        },
                        {
                            "name": "address.city",
                            "type": "Utf8",
                            "unique": False,
                            "skip_update": False
                        }
                    ]
                }
            }
        ]
    }


def generate_from_mongodb_to_mongodb_no_transformation(connectionString: str):
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
                    "unique_columns": ["id"],
                    "comparison_columns": ["id"],
                    "columns": [
                        {
                            "name": "id",
                            "type": "Int32",
                            "unique": True,
                            "skip_update": False
                        }
                    ]
                },
                "destinationConfig": {
                    "collection": "dst",
                    "unique_columns": ["id"],
                    "comparison_columns": ["id"],
                    "columns": [
                        {
                            "name": "id",
                            "type": "Int32",
                            "unique": True,
                            "skip_update": False
                        },
                    ]
                }
            }
        ]
    }
