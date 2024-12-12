def to_static() -> dict:
    return {
        "version": "V1",
        "sources": [
            {
                "name": "staticsrc",
                "type": "Static",
                "connection": {
                    "static": [
                        {"id": 1, "name": "John", "email": "john@test.com"},
                        {"id": 2, "name": "Jane", "email": "jane@test.com"},
                        {"id": 3, "name": "Steffen", "email": "steffen@test.com"},
                    ]
                },
            },
            {
                "name": "staticdst",
                "type": "Static",
                "connection": {
                    "static": [
                        {"id": 1, "name": "John", "email": "john@test.com"},
                        {"id": 4, "name": "James", "email": "jane@test.com"},
                        {"id": 3, "name": "Stephen", "email": "stephen@test.com"},
                    ]
                },
            },
        ],
        "sync": [
            {
                "source": "staticsrc",
                "destination": "staticdst",
                "sourceConfig": {},
                "destinationConfig": {},
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
                        "name": "email",
                        "type": "Utf8",
                    },
                ],
                "sourceTransformers": [],
                "destinationTransformers": [],
                "insertionTransformers": [],
            }
        ],
    }
