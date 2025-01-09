import unittest
import os
from src.beetl import beetl
from tests.configurations.xml import to_xml
from tests.helpers.manual_result import ManualResult


class TestXmlSource(unittest.TestCase):
    """Basic test of the xml source"""

    def test_sync_between_two_xml_sources(self):
        # Arrange
        source_path = "tests/configurations/xml/source-1.xml"
        destination_path = "tests/configurations/xml/destination.xml"

        self.remove_if_exists(destination_path)

        # Act
        beetl_client = beetl.Beetl(beetl.BeetlConfig(
            to_xml(source_path, destination_path)))
        create_three_result = beetl_client.sync()

        source_path = "tests/configurations/xml/source-2.xml"
        beetl_client = beetl.Beetl(beetl.BeetlConfig(
            to_xml(source_path, destination_path)))
        update_one_result = beetl_client.sync()

        source_path = "tests/configurations/xml/source-3.xml"
        beetl_client = beetl.Beetl(beetl.BeetlConfig(
            to_xml(source_path, destination_path)))
        create_one_delete_one_result = beetl_client.sync()

        # Assert
        self.assertEqual(
            create_three_result,
            ManualResult(3, 0, 0)
        )
        self.assertEqual(
            update_one_result,
            ManualResult(0, 1, 0)
        )
        self.assertEqual(
            create_one_delete_one_result,
            ManualResult(1, 0, 1)
        )

        self.remove_if_exists(destination_path)

    def remove_if_exists(self, path):
        if os.path.exists(path):
            os.remove(path)
