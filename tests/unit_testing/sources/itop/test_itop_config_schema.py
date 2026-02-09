from unittest import TestCase

from src.beetl.sources import ItopConfigArguments, ItopConfig

ArgsCLS = ItopConfigArguments
ConfCLS = ItopConfig


class UnitTestItopSyncSchema(TestCase):

    def test_sync_schema__without_soft_delete_parameter__schema_is_valid(self):
        args = {
            "name": "iTop",
            "connection": {
                "settings": {
                    "host": "itop.local",
                    "username": "Testsson",
                    "password": "Pa55w0rd",
                    "verify_ssh": "true",
                    "skip_credentials_verification": "true",
                },
            },
        }
        ConfCLS(ArgsCLS(**args))
