
from src.beetl.beetl import BeetlConfig
import unittest
from polars import Int32, Utf8


class UnitTestBeetlConfig(unittest.TestCase):

    config_with_columns_as_list = {
        "version": "V1",
        "sources": [{"name": "src", "type": "Static", "connection": {"static": [{"id": 1, "name": "foo"}]}}],
        "sync": [{
            "source": "src",
            "sourceConfig": {},
            "destination": "src",
            "destinationConfig": {},
            "comparisonColumns": [
                {
                    "name": "id",
                    "type": "Int32",
                    "unique": True
                },
                {
                    "name": "name",
                    "type": "Utf8"
                }
            ]
        }]
    }

    def test_config_init__comparison_columns_as_list__fields_are_propagated(self):
        result = BeetlConfig(self.config_with_columns_as_list)

        id = result.sync_list[0].comparisonColumns[0]
        self.assertEqual(id.name, "id")
        self.assertEqual(id.type, Int32)
        self.assertTrue(id.unique)

    def test_config_init__comparison_columns_as_list__fields_without_unique_defaults_to_false(self):
        result = BeetlConfig(self.config_with_columns_as_list)

        id = result.sync_list[0].comparisonColumns[1]
        self.assertEqual(id.name, "name")
        self.assertEqual(id.type, Utf8)
        self.assertFalse(id.unique)

    config_with_columns_as_dict = {
        "version": "V1",
        "sources": [{"name": "src", "type": "Static", "connection": {"static": [{"id": 1, "name": "foo"}]}}],
        "sync": [{
            "source": "src",
            "sourceConfig": {},
            "destination": "src",
            "destinationConfig": {},
            "comparisonColumns": {
                "id": "Int32",
                "name": "Utf8"
            }
        }]
    }

    def test_config_init__comparision_columns_as_dict__first_field_is_unique(self):
        result = BeetlConfig(self.config_with_columns_as_dict)
        id = result.sync_list[0].comparisonColumns[0]
        self.assertEqual(id.name, "id")
        self.assertEqual(id.type, Int32)
        self.assertTrue(id.unique)

    def test_config_init__comparision_columns_as_dict__second_field_is_not_unique(self):
        result = BeetlConfig(self.config_with_columns_as_dict)
        name = result.sync_list[0].comparisonColumns[1]
        self.assertEqual(name.name, "name")
        self.assertEqual(name.type, Utf8)
        self.assertFalse(name.unique)
