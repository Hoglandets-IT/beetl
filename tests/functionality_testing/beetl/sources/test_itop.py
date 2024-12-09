import unittest
from src.beetl.beetl import Beetl, BeetlConfig
from tests.configurations.itop import (
    delete_15_organizations_from_static_to_itop,
    insert_15_organizations_from_static_to_itop,
)
from tests.helpers.manual_result import ManualResult
from tests.helpers.secrets import get_test_secrets

skip_tests = False
try:
    secrets = get_test_secrets()
except:
    skip_tests = True


@unittest.skipIf(skip_tests, "No iTop secrets provided")
class TestItopSource(unittest.TestCase):
    def test_itop(self):
        # Create 14
        config_dict = insert_15_organizations_from_static_to_itop(
            secrets.itop.url, secrets.itop.username, secrets.itop.password
        )
        config = BeetlConfig(config_dict)
        beetl_instance = Beetl(config)
        created_15_result = beetl_instance.sync()

        self.assertEqual(created_15_result, ManualResult(14, 0, 0))

        # Hard delete
        config_dict = delete_15_organizations_from_static_to_itop(
            secrets.itop.url, secrets.itop.username, secrets.itop.password
        )
        config = BeetlConfig(config_dict)
        beetl_instance = Beetl(config)
        created_15_result = beetl_instance.sync()

        self.assertEqual(created_15_result, ManualResult(0, 0, 14))

        # Create 14

        # Soft delede 14

        ## There is no support for creating again after soft deleting

        # Hard delete 14
