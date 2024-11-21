import unittest
from src.beetl import beetl
from tests import configurations
from tests.helpers.sync_result import create_sync_result


class TestStaticSource(unittest.TestCase):
    """Basic test of the static source found in src/beetl/sources/static.py"""

    def setUp(self):
        self.basicConfig = configurations.from_static_to_static

    def test_static(self):
        betl = beetl.Beetl(beetl.BeetlConfig(self.basicConfig))
        amounts = betl.sync()

        self.assertEqual(
            amounts,
            create_sync_result(1, 1, 1)
        )
