import unittest
from src.beetl import beetl
from tests import configurations
from tests.helpers.manual_result import ManualResult


class TestStaticSource(unittest.TestCase):
    """Basic test of the static source found in src/beetl/sources/static.py"""

    def setUp(self):
        self.basicConfig = configurations.generate_from_static_to_static()

    def test_sync_between_two_static_sources(self):
        # Arrange
        betl = beetl.Beetl(beetl.BeetlConfig(self.basicConfig))

        # Act
        amounts = betl.sync()

        # Assert
        self.assertEqual(
            amounts,
            ManualResult(1, 1, 1)
        )
