from typing import Callable, Union
import unittest
from src.beetl.beetl import Beetl, BeetlConfig
from tests.configurations.itop import (
    delete_14_organizations_from_static_to_itop,
    delete_2_nutanix_cluster_hosts_from_static_to_itop,
    delete_2_nutanix_networks_from_static_to_itop,
    delete_2_nutanix_clusters_from_static_to_itop,
    delete_2_nutanix_virtual_machines_from_static_to_itop,
    delete_3_persons_from_static_to_itop,
    insert_14_organizations_from_static_to_itop,
    insert_2_nutanix_cluster_hosts_from_static_to_itop,
    insert_2_nutanix_networks_from_static_to_itop,
    insert_2_nutanix_clusters_from_static_to_itop,
    insert_2_nutanix_virtual_machines_from_static_to_itop,
    insert_3_persons_from_static_to_itop,
    update_2_nutanix_cluster_hosts_from_static_to_itop,
    update_2_nutanix_networks_from_static_to_itop,
    update_2_nutanix_clusters_from_static_to_itop,
    update_2_nutanix_virtual_machines_from_static_to_itop,
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

        # Clean up potenitally failed previous tests
        self.delete_organizations(skip_assertions=True)

        try:
            # Create + Delete
            self.create_organizations()
            self.delete_organizations()

            # Create + Soft Delete (update under the hood)
            self.create_organizations()
            self.delete_organizations(soft_delete=True)

        except Exception as e:
            raise e

        finally:
            # Clean up
            self.delete_organizations()

    def test_itop_persons(self):
        """This test tests that the iTop source can insert, update, and delete persons, both hard and soft."""
        try:
            # Clean up potenitally failed previous tests
            self.delete_persons(skip_assertions=True)
            self.delete_organizations(skip_assertions=True)

            # Create dependencies
            self.create_organizations()

            # Create + Delete
            self.create_persons()
            self.delete_persons()

            # Create + Soft Delete (update under the hood)
            self.create_persons()
            self.delete_persons(soft_delete=True)

        except Exception as e:
            raise e

        finally:
            # Clean up dependencies and potential failures
            self.delete_persons(skip_assertions=True)
            self.delete_organizations(skip_assertions=True)

    def test_itop_nutanix_clusters(self):
        """This test tests that the iTop source can insert, update, and delete nutanix clusters. Soft deletion is not supported for nutanix clusters."""
        try:
            # Clean up potenitally failed previous tests
            self.delete_nutanix_clusters(skip_assertions=True)
            self.delete_organizations(skip_assertions=True)

            # Create dependencies
            self.create_organizations()

            # Create + Update + Delete
            self.create_nutanix_clusters()
            self.update_nutanix_clusters()
            self.delete_nutanix_clusters()

        except Exception as e:
            raise e

        finally:
            # Clean up dependencies and potential failures
            self.delete_nutanix_clusters(skip_assertions=True)
            self.delete_organizations(skip_assertions=True)

    def test_itop_nutanix_cluster_hosts(self):
        """This test tests that the iTop source can insert, update, and delete nutanix cluster hosts. Soft deletion is not supported for nutanix clusters."""
        try:
            # Clean up potenitally failed previous tests
            self.delete_nutanix_cluster_hosts(skip_assertions=True)
            self.delete_nutanix_clusters(skip_assertions=True)
            self.delete_organizations(skip_assertions=True)

            # Create dependencies
            self.create_organizations()
            self.create_nutanix_clusters()

            # Create + Update + Delete
            self.create_nutanix_cluster_hosts()
            self.update_nutanix_cluster_hosts()
            self.delete_nutanix_cluster_hosts()

        except Exception as e:
            raise e

        finally:
            # Clean up dependencies and potential failures
            self.delete_nutanix_cluster_hosts(skip_assertions=True)
            self.delete_nutanix_clusters(skip_assertions=True)
            self.delete_organizations(skip_assertions=True)

    def test_itop_nutanix_networks(self):
        """This test tests that the iTop source can insert, update, and delete nutanix cluster networks. Soft deletion is not supported for nutanix clusters."""
        try:
            # Clean up potenitally failed previous tests
            self.delete_nutanix_cluster_networks(skip_assertions=True)
            self.delete_nutanix_clusters(skip_assertions=True)
            self.delete_organizations(skip_assertions=True)

            # Create dependencies
            self.create_organizations()
            self.create_nutanix_clusters()

            # Create + Update + Delete
            self.create_nutanix_cluster_networks()
            self.update_nutanix_cluster_networks()
            self.delete_nutanix_cluster_networks()

        except Exception as e:
            raise e

        finally:
            # Clean up dependencies and potential failures
            self.delete_nutanix_cluster_networks(skip_assertions=True)
            self.delete_nutanix_clusters(skip_assertions=True)
            self.delete_organizations(skip_assertions=True)

    def test_itop_nutanix_virtual_machines(self):
        """This test tests that the iTop source can insert, update, and delete nutanix virtual machines. Soft deletion is not supported for nutanix virtual machines."""
        try:
            # Clean up potenitally failed previous tests
            self.delete_nutanix_virtual_machines(skip_assertions=True)
            self.delete_nutanix_clusters(skip_assertions=True)
            self.delete_organizations(skip_assertions=True)

            # Create dependencies
            self.create_organizations()
            self.create_nutanix_clusters()

            # Create + Update + Delete
            self.create_nutanix_virtual_machines()
            self.update_nutanix_virtual_machines()
            self.delete_nutanix_virtual_machines()

        except Exception as e:
            raise e

        finally:
            # Clean up dependencies and potential failures
            self.delete_nutanix_virtual_machines(skip_assertions=True)
            self.delete_nutanix_clusters(skip_assertions=True)
            self.delete_organizations(skip_assertions=True)

    def run_sync(
        self: "TestItopSource",
        config_generator: Callable[[str, str, str, bool], dict],
        soft_delete: bool = False,
    ):
        config_dict = config_generator(
            secrets.itop.url,
            secrets.itop.username,
            secrets.itop.password,
            soft_delete=soft_delete,
        )
        config = BeetlConfig(config_dict)
        beetl_instance = Beetl(config)
        result = beetl_instance.sync()
        return result

    def delete_organizations(
        self, soft_delete: bool = False, skip_assertions: bool = False
    ):
        config_dict = delete_14_organizations_from_static_to_itop(
            secrets.itop.url,
            secrets.itop.username,
            secrets.itop.password,
            soft_delete=soft_delete,
        )
        config = BeetlConfig(config_dict)
        beetl_instance = Beetl(config)
        deleted_14_result = beetl_instance.sync()

        if skip_assertions:
            return

        self.assertEqual(deleted_14_result, ManualResult(0, 0, 14))

    def create_organizations(
        self, soft_delete: bool = False, skip_assertions: bool = False
    ):
        config_dict = insert_14_organizations_from_static_to_itop(
            secrets.itop.url,
            secrets.itop.username,
            secrets.itop.password,
            soft_delete=soft_delete,
        )
        config = BeetlConfig(config_dict)
        beetl_instance = Beetl(config)
        created_15_result = beetl_instance.sync()

        if skip_assertions:
            return

        self.assertEqual(created_15_result, ManualResult(14, 0, 0))

    def create_persons(self, soft_delete: bool = False, skip_assertions: bool = False):
        config_dict = insert_3_persons_from_static_to_itop(
            secrets.itop.url,
            secrets.itop.username,
            secrets.itop.password,
            soft_delete=soft_delete,
        )
        config = BeetlConfig(config_dict)
        beetl_instance = Beetl(config)
        created_15_result = beetl_instance.sync()

        if skip_assertions:
            return

        self.assertEqual(created_15_result, ManualResult(3, 0, 0))

    def delete_persons(self, soft_delete: bool = False, skip_assertions: bool = False):
        config_dict = delete_3_persons_from_static_to_itop(
            secrets.itop.url,
            secrets.itop.username,
            secrets.itop.password,
            soft_delete=soft_delete,
        )
        config = BeetlConfig(config_dict)
        beetl_instance = Beetl(config)
        created_15_result = beetl_instance.sync()

        if skip_assertions:
            return

        self.assertEqual(created_15_result, ManualResult(0, 0, 3))

    def create_nutanix_clusters(
        self, soft_delete: bool = False, skip_assertions: bool = False
    ):
        config_generator = insert_2_nutanix_clusters_from_static_to_itop
        expected_result = ManualResult(2, 0, 0)
        result = self.run_sync(config_generator, soft_delete=soft_delete)

        if skip_assertions:
            return

        self.assertEqual(result, expected_result)

    def update_nutanix_clusters(
        self, soft_delete: bool = False, skip_assertions: bool = False
    ):
        config_generator = update_2_nutanix_clusters_from_static_to_itop
        expected_result = ManualResult(0, 2, 0)
        result = self.run_sync(config_generator, soft_delete=soft_delete)

        if skip_assertions:
            return

        self.assertEqual(result, expected_result)

    def delete_nutanix_clusters(
        self, soft_delete: bool = False, skip_assertions: bool = False
    ):
        config_generator = delete_2_nutanix_clusters_from_static_to_itop
        expected_result = ManualResult(0, 0, 2)
        result = self.run_sync(config_generator, soft_delete=soft_delete)

        if skip_assertions:
            return

        self.assertEqual(result, expected_result)

    def create_nutanix_cluster_hosts(
        self, soft_delete: bool = False, skip_assertions: bool = False
    ):
        config_generator = insert_2_nutanix_cluster_hosts_from_static_to_itop
        expected_result = ManualResult(2, 0, 0)
        result = self.run_sync(config_generator, soft_delete=soft_delete)

        if skip_assertions:
            return

        self.assertEqual(result, expected_result)

    def update_nutanix_cluster_hosts(
        self, soft_delete: bool = False, skip_assertions: bool = False
    ):
        config_generator = update_2_nutanix_cluster_hosts_from_static_to_itop
        expected_result = ManualResult(0, 2, 0)
        result = self.run_sync(config_generator, soft_delete=soft_delete)

        if skip_assertions:
            return

        self.assertEqual(result, expected_result)

    def delete_nutanix_cluster_hosts(
        self, soft_delete: bool = False, skip_assertions: bool = False
    ):
        config_generator = delete_2_nutanix_cluster_hosts_from_static_to_itop
        expected_result = ManualResult(0, 0, 2)
        result = self.run_sync(config_generator, soft_delete=soft_delete)

        if skip_assertions:
            return

        self.assertEqual(result, expected_result)

    def create_nutanix_cluster_networks(
        self, soft_delete: bool = False, skip_assertions: bool = False
    ):
        config_generator = insert_2_nutanix_networks_from_static_to_itop
        expected_result = ManualResult(2, 0, 0)
        result = self.run_sync(config_generator, soft_delete=soft_delete)

        if skip_assertions:
            return

        self.assertEqual(result, expected_result)

    def update_nutanix_cluster_networks(
        self, soft_delete: bool = False, skip_assertions: bool = False
    ):
        config_generator = update_2_nutanix_networks_from_static_to_itop
        expected_result = ManualResult(0, 2, 0)
        result = self.run_sync(config_generator, soft_delete=soft_delete)

        if skip_assertions:
            return

        self.assertEqual(result, expected_result)

    def delete_nutanix_cluster_networks(
        self, soft_delete: bool = False, skip_assertions: bool = False
    ):
        config_generator = delete_2_nutanix_networks_from_static_to_itop
        expected_result = ManualResult(0, 0, 2)
        result = self.run_sync(config_generator, soft_delete=soft_delete)

        if skip_assertions:
            return

        self.assertEqual(result, expected_result)

    def create_nutanix_virtual_machines(
        self, soft_delete: bool = False, skip_assertions: bool = False
    ):
        config_generator = insert_2_nutanix_virtual_machines_from_static_to_itop
        expected_result = ManualResult(2, 0, 0)
        result = self.run_sync(config_generator, soft_delete=soft_delete)

        if skip_assertions:
            return

        self.assertEqual(result, expected_result)

    def update_nutanix_virtual_machines(
        self, soft_delete: bool = False, skip_assertions: bool = False
    ):
        config_generator = update_2_nutanix_virtual_machines_from_static_to_itop
        expected_result = ManualResult(0, 2, 0)
        result = self.run_sync(config_generator, soft_delete=soft_delete)

        if skip_assertions:
            return

        self.assertEqual(result, expected_result)

    def delete_nutanix_virtual_machines(
        self, soft_delete: bool = False, skip_assertions: bool = False
    ):
        config_generator = delete_2_nutanix_virtual_machines_from_static_to_itop
        expected_result = ManualResult(0, 0, 2)
        result = self.run_sync(config_generator, soft_delete=soft_delete)

        if skip_assertions:
            return

        self.assertEqual(result, expected_result)
