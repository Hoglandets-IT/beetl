import unittest
from src.beetl import beetl
from tests.configurations.xml import to_xml
from tests.helpers.manual_result import ManualResult


class TestStaticSource(unittest.TestCase):
    """Basic test of the xml source"""

    def test_sync_between_two_xml_sources(self):
        # Arrange
        source_path = "tests/configurations/xml/source.xml"
        destination_path = "tests/configurations/xml/destination.xml"
        beetl_client = beetl.Beetl(beetl.BeetlConfig(
            to_xml(source_path, destination_path)))

        # Act
        result = beetl_client.sync()

        # Assert
        self.assertEqual(
            result,
            ManualResult(1, 1, 1)
        )
