from unittest import TestCase

from src.beetl.config.config_base import BeetlConfig


class DiffIntegrationTests(TestCase):
    def test_diff_config(self):
        dict_config = {}
        config = BeetlConfig(config)
