import os
from unittest import TestCase

from polars import read_csv

from src.beetl.beetl import Beetl
from src.beetl.config import BeetlConfig
from tests.helpers.temp import TEMP_PATH, clean_temp_directory, create_temp_file


class TestCsvSource(TestCase):
    def test_save_diff__when_called__saves_diff_to_file(self):
        clean_temp_directory()
        try:
            diff_file_name = "diff.csv"
            create_temp_file(diff_file_name)

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
                            ]
                        },
                    },
                    {
                        "type": "Csv",
                        "name": "diff",
                        "connection": {
                            "path": os.path.join(TEMP_PATH, diff_file_name),
                            "encoding": "utf-8",
                        },
                    },
                    {
                        "type": "Static",
                        "name": "destination",
                        "connection": {
                            "static": [
                                {"id": 3, "name": "test", "age": 20},
                                {"id": 4, "name": "test4", "age": 20},
                                {"id": 5, "name": "test5", "age": 20},
                            ]
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
                                "config": {},
                            }
                        },
                    }
                ],
            }
            beetl_config_instance = BeetlConfig(config_dict)
            beetl_instance = Beetl(beetl_config_instance)

            beetl_instance.sync()

            result = read_csv(os.path.join(TEMP_PATH, diff_file_name))
            self.assertEqual(result.height, 1)
        finally:
            clean_temp_directory()
