def diff_to_faker() -> dict:
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Faker",
                "connection": {
                    "faker": [
                        {"id": 1, "name": "John", "email": "john@test.com"},
                        {"id": 2, "name": "Jane", "email": "jane@test.com"},
                        {"id": 3, "name": "Steffen", "email": "steffen@test.com"},
                    ]
                },
            },
            {
                "name": "dst",
                "type": "Faker",
                "connection": {
                    "faker": [
                        {"id": 1, "name": "John", "email": "john@test.com"},
                        {"id": 4, "name": "James", "email": "jane@test.com"},
                        {"id": 3, "name": "Stephen", "email": "stephen@test.com"},
                    ]
                },
            },
        ],
        "sync": [
            {
                "source": "src",
                "destination": "dst",
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
                "diff": {"destination": {"name": "dst", "type": "Faker", "config": {}}},
            }
        ],
    }
