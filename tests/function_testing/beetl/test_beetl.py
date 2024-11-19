import os
import json
import yaml
import unittest
from polars import DataFrame
from src.beetl import beetl, config
from src.beetl.sources import interface as src_if
from tests import configurations


class TestBeetlFunctions(unittest.TestCase):
    """Basic functionality test for functions in src/beetl/beetl.py"""

    def setUp(self):
        self.basicConfig = configurations.from_static_to_static

        if not os.path.isdir('/tmp/beetl'):
            os.mkdir('/tmp/beetl')

        with open('/tmp/beetl/test-basic-config.yaml', 'w') as f:
            yaml.dump(self.basicConfig, f)

        with open('/tmp/beetl/test-basic-config.json', 'w') as f:
            json.dump(self.basicConfig, f)

    def assertConfig(self, beetl_config: config.BeetlConfig):
        direct_config = config.BeetlConfig(self.basicConfig)

        self.assertIsInstance(beetl_config, config.BeetlConfigV1)
        self.assertEqual(beetl_config.version, 'V1')

        self.assertEqual(len(beetl_config.sources), 2)
        for key in beetl_config.sources.keys():
            self.assertIsInstance(
                beetl_config.sources[key], src_if.SourceInterface)

            self.assertEqual(
                beetl_config.sources[key].__dict__,
                direct_config.sources[key].__dict__
            )

        for sync, _ in enumerate(beetl_config.sync_list):
            for transformer, _ in enumerate(beetl_config.sync_list[sync].destinationTransformers):
                self.assertEqual(
                    beetl_config.sync_list[sync].destinationTransformers[transformer].__dict__,
                    direct_config.sync_list[sync].destinationTransformers[transformer].__dict__
                )

    def test_from_yaml_file(self):
        beetl_config = beetl.BeetlConfig.from_yaml_file(
            '/tmp/beetl/test-basic-config.yaml')
        self.assertConfig(beetl_config)

    def test_from_json_file(self):
        beetl_config = beetl.BeetlConfig.from_json_file(
            '/tmp/beetl/test-basic-config.json')
        self.assertConfig(beetl_config)

    def test_from_yaml(self):
        with open('/tmp/beetl/test-basic-config.yaml', 'r') as f:
            beetl_config = beetl.BeetlConfig.from_yaml(f.read())

        self.assertConfig(beetl_config)

    def test_from_json(self):
        with open('/tmp/beetl/test-basic-config.json', 'r') as f:
            beetl_config = beetl.BeetlConfig.from_json(f.read())

        self.assertConfig(beetl_config)

    def test_compare_datasets(self):
        source_data = DataFrame(
            self.basicConfig['sources'][0]['connection']['static'])
        dest_data = DataFrame(
            self.basicConfig['sources'][1]['connection']['static'])

        insert, update, delete = beetl.Beetl.compare_datasets(
            source_data, dest_data)

        self.assertEqual(
            insert.to_dict(as_series=False),
            {
                'id': [2],
                'name': ['Jane'],
                'email': ['jane@test.com']
            }
        )

        self.assertEqual(
            update.to_dict(as_series=False),
            {
                'id': [3],
                'name': ['Steffen'],
                'email': ['steffen@test.com']
            }
        )

        self.assertEqual(
            delete.to_dict(as_series=False),
            {
                'id': [4]
            }
        )
