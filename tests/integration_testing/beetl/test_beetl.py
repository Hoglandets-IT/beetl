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

    def test_compare_datasets(self):
        source_data = DataFrame(self.basicConfig["sources"][0]["connection"]["static"])
        dest_data = DataFrame(self.basicConfig["sources"][1]["connection"]["static"])

        columns = [
            config.ComparisonColumn(name="id", type="Int64", unique=True),
            config.ComparisonColumn(name="name", type="Utf8"),
            config.ComparisonColumn(name="email", type="Utf8"),
        ]
        insert, update, delete = beetl.Beetl.compare_datasets(
            source_data, dest_data, ["id"], columns
        )

        self.assertEqual(
            insert.to_dict(as_series=False),
            {"id": [2], "name": ["Jane"], "email": ["jane@test.com"]},
        )

        self.assertEqual(
            update.to_dict(as_series=False),
            {"id": [3], "name": ["Steffen"], "email": ["steffen@test.com"]},
        )

        self.assertEqual(
            delete.to_dict(as_series=False),
            {"id": [4], "name": ["James"], "email": ["jane@test.com"]},
        )

    def test_dry_run_sync(self):

        beetl_config = beetl.BeetlConfig(to_static())
        beetl_instance = beetl.Beetl(beetl_config)
        comparison_results = beetl_instance.sync(dry_run=True)

        self.assertTrue(len(comparison_results) > 0)
        result = comparison_results[0]

        self.assertTrue(isinstance(result, ComparisonResult))
