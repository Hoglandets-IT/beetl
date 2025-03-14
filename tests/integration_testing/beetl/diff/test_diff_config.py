from unittest import TestCase

from src.beetl.beetl import Beetl, BeetlConfig


class DiffIntegrationTests(TestCase):
    def test_diff_config(self):
        dict_config = {
            "version": "V1",
            "sources": [
                {
                    "name": "src",
                    "type": "Static",
                    "connection": {
                        "static": [
                            {"id": 1, "name": "test1"},
                            {"id": 2, "name": "test2"},
                            {"id": 3, "name": "test3"},
                            {"id": 4, "name": "test4"},
                        ]
                    },
                },
                {
                    "name": "dst",
                    "type": "Static",
                    "connection": {
                        "static": [
                            {"id": 5, "name": "test5"},
                        ]
                    },
                },
            ],
            "sync": [
                {
                    "name": "diff test",
                    "source": "src",
                    "destination": "dst",
                    "sourceConfig": {},
                    "destinationConfig": {},
                    "comparisonColumns": [
                        {"name": "id", "type": "Int64", "unique": True}
                    ],
                    "diff": {"type": "Static", "name": "dst", "config": {}},
                }
            ],
        }
        config = BeetlConfig(dict_config)
        result = Beetl(config).sync()
        print(result)
