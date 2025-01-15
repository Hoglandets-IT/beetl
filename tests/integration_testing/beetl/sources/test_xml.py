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

    def test_transforming_xml_using_xsl(self):
        source_path = "tests/configurations/xml/nested-data.xml"
        destination_path = "tests/configurations/xml/destination.xml"
        self.remove_if_exists(destination_path)

        # Transforms persons with addresses to addresses with person names
        xsl = """<?xml version="1.0" encoding="UTF-8"?>
        <xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
            <xsl:output method="xml" indent="yes"/>
            <xsl:template match="//body">
                <Output>
                    <xsl:for-each select=".//person">
                        <xsl:variable name="person_name" select=".//name"/>
        
                        <xsl:for-each select=".//address">
                            <Record>
                                <name><xsl:value-of select="$person_name"/></name>
                                <street><xsl:value-of select=".//street"/></street>
                            </Record>
                        </xsl:for-each>
                    </xsl:for-each>
                </Output>
            </xsl:template>
        </xsl:stylesheet>"""

        config = {
            "version": "V1",
            "sources": [
                {
                    "name": "src",
                    "type": "Xml",
                    "connection": {
                        "path": source_path,
                    },
                },
                {
                    "name": "dst",
                    "type": "Xml",
                    "connection": {
                        "path": destination_path,
                    },
                },
            ],
            "sync": [
                {
                    "source": "src",
                    "destination": "dst",
                    "sourceConfig": {
                        "xsl": xsl
                    },
                    "destinationConfig": {
                        "xpath": ".//address",
                        "root_name": "addressexport",
                        "row_name": "address",
                        "unique_columns": ("name")
                    },
                    "comparisonColumns": [
                        {
                            "name": "name",
                            "type": "Utf8",
                            "unique": True
                        },
                    ],
                    "sourceTransformers": [],
                    "destinationTransformers": [],
                    "insertionTransformers": [],
                }
            ]
        }

        beetl_client = beetl.Beetl(beetl.BeetlConfig(config))

        result = beetl_client.sync(dry_run=True)[0].create

        self.assertEqual(result.height, 4)
        self.assertSequenceEqual(result.columns, ["name", "street"])
        self.assertEqual(result.count()["name"][0], 4)
        self.assertEqual(result.count()["street"][0], 4)

        self.remove_if_exists(destination_path)
