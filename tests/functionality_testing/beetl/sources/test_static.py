import unittest
from src.beetl import beetl
from tests.configurations.static import to_static
from tests.helpers.manual_result import ManualResult


class TestStaticSource(unittest.TestCase):
    """Basic test of the static source found in src/beetl/sources/static.py"""

    def test_sync_between_two_static_sources(self):
        # Arrange
        betl = beetl.Beetl(beetl.BeetlConfig(to_static()))

        # Act
        amounts = betl.sync()

        # Assert
        self.assertEqual(
            amounts,
            ManualResult(1, 1, 1)
        )
