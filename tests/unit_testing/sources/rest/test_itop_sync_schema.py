from unittest import TestCase

from src.beetl.sources import ItopSyncArguments

CLS = ItopSyncArguments


class UnitTestItopSyncSchema(TestCase):
    metadata = {
        "location": ("sync", str(0), "sourceConfig"),
        "name": "src",
        "direction": "source",
    }

    def test_sync_schema__without_soft_delete_parameter__schema_is_valid(self):
        args = {
            "datamodel": "value",
            "oql_key": "value",
            "link_columns": ["value"],
            "comparison_columns": ["value"],
            "unique_columns": ["value"],
            "skip_columns": ["value"],
        }
        CLS(**args, **self.metadata)

    def test_sync_schema__with_soft_delete_parameter__schema_is_valid(self):
        args = {
            "datamodel": "value",
            "oql_key": "value",
            "link_columns": ["value"],
            "comparison_columns": ["value"],
            "unique_columns": ["value"],
            "skip_columns": ["value"],
            "soft_delete": {
                "enabled": True,
                "field": "value",
                "active_value": "value",
                "inactive_value": "value",
            },
        }
        CLS(**args, **self.metadata)
