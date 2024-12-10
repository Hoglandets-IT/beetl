import unittest
from src.beetl.beetl import Beetl, BeetlConfig
from tests.configurations.itop import (
    delete_14_organizations_from_static_to_itop,
    insert_14_organizations_from_static_to_itop,
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
    """Since iTop isnt't easy to set up as a 3rd party dependency container for testing you have to provide the hostname and credentials to your test instance in the test.secrets.yaml file for now."""

    def test_itop_organizations(self):
        """This test tests that the iTop source can insert, update, and delete organizations, both hard and soft."""
        # Clean up any existing test organizations from previous failed runs.
        config_dict = delete_14_organizations_from_static_to_itop(
            secrets.itop.url,
            secrets.itop.username,
            secrets.itop.password,
            soft_delete=False,
        )
        config = BeetlConfig(config_dict)
        beetl_instance = Beetl(config)
        beetl_instance.sync()

        # Create 14 organizations
        config_dict = insert_14_organizations_from_static_to_itop(
            secrets.itop.url,
            secrets.itop.username,
            secrets.itop.password,
            soft_delete=False,
        )
        config = BeetlConfig(config_dict)
        beetl_instance = Beetl(config)
        created_15_result = beetl_instance.sync()

        self.assertEqual(created_15_result, ManualResult(14, 0, 0))

        # Hard delete 14 organizations
        config_dict = delete_14_organizations_from_static_to_itop(
            secrets.itop.url,
            secrets.itop.username,
            secrets.itop.password,
            soft_delete=False,
        )
        config = BeetlConfig(config_dict)
        beetl_instance = Beetl(config)
        deleted_14_result = beetl_instance.sync()

        self.assertEqual(deleted_14_result, ManualResult(0, 0, 14))

        # Create 14 organizations
        config_dict = insert_14_organizations_from_static_to_itop(
            secrets.itop.url,
            secrets.itop.username,
            secrets.itop.password,
            soft_delete=True,
        )
        config = BeetlConfig(config_dict)
        beetl_instance = Beetl(config)
        created_15_result = beetl_instance.sync()

        self.assertEqual(created_15_result, ManualResult(14, 0, 0))

        # Update (soft delete) 14 organizations
        config_dict = delete_14_organizations_from_static_to_itop(
            secrets.itop.url,
            secrets.itop.username,
            secrets.itop.password,
            soft_delete=True,
        )
        config = BeetlConfig(config_dict)
        beetl_instance = Beetl(config)
        updated_14_result = beetl_instance.sync()
        self.assertEqual(updated_14_result, ManualResult(0, 0, 14))

        # Hard delete 14 organizations
        config_dict = delete_14_organizations_from_static_to_itop(
            secrets.itop.url,
            secrets.itop.username,
            secrets.itop.password,
            soft_delete=False,
        )
        config = BeetlConfig(config_dict)
        beetl_instance = Beetl(config)
        deleted_14_result = beetl_instance.sync()

        self.assertEqual(deleted_14_result, ManualResult(0, 0, 14))
