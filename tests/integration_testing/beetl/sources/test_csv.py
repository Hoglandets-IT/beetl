from unittest import TestCase

from src.beetl.beetl import Beetl
from src.beetl.config import BeetlConfig


class TestCsvSource(TestCase):
    def test_save_diff__when_called__saves_diff_to_file(self):

        config_dict = {
            "version": "V1",
            "sources": [
                {
                    "name": "source",
                    "type": "Static",
                    "connection": {
                        "static": [
                            {"id": 2, "name": "test2", "age": 20},
                            {"id": 3, "name": "test3", "age": 20},
                            {"id": 4, "name": "test4", "age": 20},
                            {"id": 5, "name": "test5", "age": 20},
                        ]
                    },
                },
                {
                    "type": "Csv",
                    "name": "diff",
                    "connection": {
                        "path": "./tests/.output/diff.csv",
                        "encoding": "utf-8",
                    },
                },
                {
                    "type": "Csv",
                    "name": "destination",
                    "connection": {
                        "path": "./tests/.output/output.csv",
                        "encoding": "utf-8",
                    },
                },
            ],
            "sync": [
                {
                    "name": "test_diff",
                    "source": "source",
                    "destination": "destination",
                    "sourceConfig": {},
                    "destinationConfig": {},
                    "comparisonColumns": [
                        {"name": "id", "type": "Int64", "unique": True},
                        {"name": "name", "type": "Utf8"},
                        {"name": "age", "type": "Int64"},
                    ],
                    "diff": {
                        "destination": {
                            "type": "Csv",
                            "name": "diff",
                            "config": {"path": "./tests/.output/diff.csv"},
                        }
                    },
                }
            ],
        }
        beetl_config_instance = BeetlConfig(config_dict)
        beetl_instance = Beetl(beetl_config_instance)

        beetl_instance.sync()
