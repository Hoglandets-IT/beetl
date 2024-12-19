from beetl.beetl import Beetl, BeetlConfig, Result


def distribution_test():
    """Simple test just to make sure that we can import beetl, create a config and run the sync.
    It uses the installed version of beetl, not the source files."""
    config = BeetlConfig(get_config())
    beetl = Beetl(config)
    result = beetl.sync()

    if not result or result != ManualResult(1, 1, 1):
        raise Exception("Distribution test failed")


def get_config():
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


class ManualResult(Result):
    def __init__(self, creates, updates, deletes):
        self.inserts = creates
        self.updates = updates
        self.deletes = deletes


if __name__ == "__main__":
    distribution_test()
