import json
import os
import unittest
from copy import deepcopy

import yaml
from polars import DataFrame

from src.beetl import beetl, config
from src.beetl.comparison_result import ComparisonResult
from src.beetl.sources import interface as src_if
from tests.configurations.static import to_static


class TestBeetlFunctions(unittest.TestCase):
    """Basic functionality test for functions in src/beetl/beetl.py"""

    @classmethod
    def setUpClass(self):
        self.basicConfig = to_static()

        if not os.path.isdir("/tmp/beetl"):
            os.mkdir("/tmp/beetl")

        with open("/tmp/beetl/test-basic-config.yaml", "w") as f:
            yaml.dump(self.basicConfig, f)

        with open("/tmp/beetl/test-basic-config.json", "w") as f:
            json.dump(self.basicConfig, f)

    def test_from_yaml_file(self):
        beetl_config = beetl.BeetlConfig.from_yaml_file(
            "/tmp/beetl/test-basic-config.yaml"
        )
        self.assertIsNotNone(beetl_config)
        self.assertEqual(len(beetl_config.sources), 2)
        self.assertEqual(len(beetl_config.sync_list), 1)
        self.assertEqual(len(beetl_config.sync_list[0].sourceTransformers), 0)
        self.assertEqual(len(beetl_config.sync_list[0].destinationConfig), 0)
        self.assertEqual(len(beetl_config.sync_list[0].deletionTransformers), 0)

    def test_from_json_file(self):
        beetl_config = beetl.BeetlConfig.from_json_file(
            "/tmp/beetl/test-basic-config.json"
        )
        self.assertIsNotNone(beetl_config)
        self.assertEqual(len(beetl_config.sources), 2)
        self.assertEqual(len(beetl_config.sync_list), 1)
        self.assertEqual(len(beetl_config.sync_list[0].sourceTransformers), 0)
        self.assertEqual(len(beetl_config.sync_list[0].destinationConfig), 0)
        self.assertEqual(len(beetl_config.sync_list[0].deletionTransformers), 0)

    def test_from_yaml(self):
        with open("/tmp/beetl/test-basic-config.yaml", "r") as f:
            beetl_config = beetl.BeetlConfig.from_yaml(f.read())

        self.assertIsNotNone(beetl_config)
        self.assertEqual(len(beetl_config.sources), 2)
        self.assertEqual(len(beetl_config.sync_list), 1)
        self.assertEqual(len(beetl_config.sync_list[0].sourceTransformers), 0)
        self.assertEqual(len(beetl_config.sync_list[0].destinationConfig), 0)
        self.assertEqual(len(beetl_config.sync_list[0].deletionTransformers), 0)

    def test_from_json(self):
        with open("/tmp/beetl/test-basic-config.json", "r") as f:
            beetl_config = beetl.BeetlConfig.from_json(f.read())

        self.assertIsNotNone(beetl_config)
        self.assertEqual(len(beetl_config.sources), 2)
        self.assertEqual(len(beetl_config.sync_list), 1)
        self.assertEqual(len(beetl_config.sync_list[0].sourceTransformers), 0)
        self.assertEqual(len(beetl_config.sync_list[0].destinationConfig), 0)
        self.assertEqual(len(beetl_config.sync_list[0].deletionTransformers), 0)

    def test_dry_run_sync(self):

        beetl_config = beetl.BeetlConfig(to_static())
        beetl_instance = beetl.Beetl(beetl_config)
        comparison_results = beetl_instance.sync(dry_run=True)

        self.assertTrue(len(comparison_results) > 0)
        result = comparison_results[0]

        self.assertTrue(isinstance(result, ComparisonResult))

    def test_generate_update_diff_sync(self):
        # Arrange
        beetl_config = beetl.BeetlConfig(to_static())
        beetl_instance = beetl.Beetl(beetl_config)

        # Act
        comparison_results = beetl_instance.sync(generate_update_diff=True)

        # Assert
        self.assertTrue(len(comparison_results) > 0)

        diff = comparison_results[0][0][0]
        self.assertIsNotNone(diff)

        changed_columns = comparison_results[0][1]
        self.assertIsNotNone(changed_columns)
