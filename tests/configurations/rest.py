
def to_static(base_url: str, path: str):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Rest",
                "connection": {
                    "settings": {
                        "base_url": base_url,
                        "authentication": None,
                        "ignore_certificates": True
                    }
                }
            },
            {
                "name": "dst",
                "type": "Static",
                "connection": {
                    "static": []
                }
            },
        ],
        "sync": [
            {
                "source": "src",
                "sourceConfig": {
                    "listRequest": {
                        "path": path
                    }
                },
                "destination": "dst",
                "destinationConfig": {},
                "comparisonColumns": [
                    {
                        "name": "id",
                        "type": "Utf8",
                        "unique": True
                    },
                    {
                        "name": "name",
                        "type": "Utf8"
                    },
                    {
                        "name": "email",
                        "type": "Utf8"
                    },
                    {
                        "name": "address.city",
                        "type": "Utf8"
                    },
                    {
                        "name": "address.street",
                        "type": "Utf8"
                    }
                ]
            }
        ]
    }
