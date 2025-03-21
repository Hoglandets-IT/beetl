from unittest import TestCase

from src.beetl.beetl import Beetl, BeetlConfig


class DiffIntegrationTests(TestCase):
    def test_static_diff_config(self):
        dict_config = {
            "version": "V1",
            "sources": [
                {
                    "name": "src",
                    "type": "Static",
                    "connection": {
                        "static": [
                            {"id": 1, "name": "test1", "age": 20},
                            {"id": 2, "name": "test2", "age": 20},
                            {"id": 3, "name": "test3", "age": 20},
                            {"id": 4, "name": "test4", "age": 20},
                            {"id": 6, "name": "test6", "age": 20},
                            {"id": 7, "name": "test7", "age": 20},
                        ]
                    },
                },
                {
                    "name": "dst",
                    "type": "Static",
                    "connection": {
                        "static": [
                            {"id": 5, "name": "test5", "age": 20},
                            {"id": 6, "name": "test", "age": 20},
                            {"id": 7, "name": "test", "age": 21},
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
                    "comparisonColumns": {
                        "id": "Int64",
                        "name": "Utf8",
                        "age": "Int64",
                    },
                    "diff": {"type": "Static", "name": "dst", "config": {}},
                }
            ],
        }
        config = BeetlConfig(dict_config)
        result = Beetl(config).sync()
        print(result)
