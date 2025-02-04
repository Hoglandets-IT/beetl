from unittest import TestCase

from src.beetl.errors import ConfigValidationError, ConfigValueError
from src.beetl.sources import RestSyncArguments


class UnitTestRestSyncSchema(TestCase):
    metadata = {
        "location": ("sync", str(0), "sourceConfig"),
        "name": "src",
        "direction": "source",
    }

    def test_sync_schema__with_string_body__schema_is_valid(self):
        args = {
            "listRequest": {
                "path": "https://api.example.com",
                "method": "GET",
                "body_type": "application/json",
                "body": {"key": "value"},
            }
        }
        RestSyncArguments(**args, **self.metadata)

    def test_sync_schema__with_dict_body__schema_is_valid(self):
        args = {
            "listRequest": {
                "path": "https://api.example.com",
                "method": "GET",
                "body_type": "application/text",
                "body": "body-string",
            }
        }
        RestSyncArguments(**args, **self.metadata)

    def test_sync_schema__with_string_body_and_body_type_application_json__raises_validation_exception(
        self,
    ):
        args = {
            "listRequest": {
                "path": "https://api.example.com",
                "method": "GET",
                "body_type": "application/json",
                "body": "body-string",
            }
        }

        self.assertRaises(
            ConfigValueError, lambda: RestSyncArguments(**args, **self.metadata)
        )

    def test_sync_schema__with_response__schema_is_valid(self):
        args = {
            "listRequest": {
                "path": "https://api.example.com",
                "method": "GET",
                "body_type": "application/json",
                "body": {"key": "value"},
                "response": {"length": "metadata.total_machines", "items": "entities"},
            }
        }
        RestSyncArguments(**args, **self.metadata)
