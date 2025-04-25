import unittest

from src.beetl import beetl
from tests.configurations.faker import diff_to_faker


class TestDiffSource(unittest.TestCase):
    """Basic test of the faker source found"""

    def test_store_diff(self):
        """Even though this does not assert anything there should be at least one test using the faker source to make sure it doesnt break"""
        # Arrange
        betl = beetl.Beetl(beetl.BeetlConfig(diff_to_faker()))

        # Act
        betl.sync()
