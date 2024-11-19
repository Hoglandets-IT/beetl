from_static_to_static = {
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
