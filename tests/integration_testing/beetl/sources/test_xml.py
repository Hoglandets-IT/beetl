import os
import unittest

import pandas as pd
import polars as pl

from src.beetl import beetl
from src.beetl.config import BeetlConfig
from tests.configurations.xml import diff_to_xml, to_xml
from tests.helpers.manual_result import ManualResult
from tests.helpers.temp import clean_temp_directory, create_temp_file


class TestXmlSource(unittest.TestCase):
    """Basic test of the xml source"""

    def test_sync_between_two_xml_sources(self):
        # Arrange
        source_path = "tests/configurations/xml/source-1.xml"
        destination_path = "tests/configurations/xml/destination.xml"

        self._remove_if_exists(destination_path)

        # Act
        beetl_client = beetl.Beetl(
            beetl.BeetlConfig(to_xml(source_path, destination_path))
        )
        create_three_result = beetl_client.sync()

        source_path = "tests/configurations/xml/source-2.xml"
        beetl_client = beetl.Beetl(
            beetl.BeetlConfig(to_xml(source_path, destination_path))
        )
        update_one_result = beetl_client.sync()

        source_path = "tests/configurations/xml/source-3.xml"
        beetl_client = beetl.Beetl(
            beetl.BeetlConfig(to_xml(source_path, destination_path))
        )
        create_one_delete_one_result = beetl_client.sync()

        # Assert
        self.assertEqual(create_three_result, ManualResult(3, 0, 0))
        self.assertEqual(update_one_result, ManualResult(0, 1, 0))
        self.assertEqual(create_one_delete_one_result, ManualResult(1, 0, 1))

        self._remove_if_exists(destination_path)

    def _remove_if_exists(self, path):
        if os.path.exists(path):
            os.remove(path)

    def test_transforming_xml_using_xsl(self):
        source_path = "tests/configurations/xml/nested-data.xml"
        destination_path = "tests/configurations/xml/destination.xml"
        self._remove_if_exists(destination_path)

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
                    "sourceConfig": {"xsl": xsl},
                    "destinationConfig": {
                        "xpath": ".//address",
                        "root_name": "addressexport",
                        "row_name": "address",
                        "unique_columns": ("name",),
                    },
                    "comparisonColumns": [
                        {"name": "name", "type": "Utf8", "unique": True},
                    ],
                    "sourceTransformers": [],
                    "destinationTransformers": [],
                    "insertionTransformers": [],
                }
            ],
        }

        beetl_client = beetl.Beetl(beetl.BeetlConfig(config))

        result = beetl_client.sync(dry_run=True)[0].create

        self.assertEqual(4, result.height)
        self.assertSequenceEqual(["name", "street"], result.columns)
        self.assertEqual(4, result.count()["name"][0])
        self.assertEqual(4, result.count()["street"][0])

        self._remove_if_exists(destination_path)

    def test_store_diff__when_diff_is_configured__stores_diff_in_file(self):
        # Arrange
        clean_temp_directory()

        diff_path = create_temp_file("xml_diff.xml")
        beetl_client = beetl.Beetl(BeetlConfig(diff_to_xml(diff_path)))

        # Act
        beetl_client.sync()

        # Assert
        ## Assert that one diff was created
        result = pl.from_pandas(pd.read_xml(diff_path))
        self.assertEqual(result.height, 1)
        for value in result.to_dicts()[0].values():
            self.assertIsNotNone(value, "All values should be present in the diff file")

        ## Insert another diff into the existing file and assert that it was appended
        beetl_client.sync()
        result = pl.from_pandas(pd.read_xml(diff_path))
        self.assertEqual(result.height, 2)
        for dictionary in result.to_dicts():
            for value in dictionary.values():
                self.assertIsNotNone(
                    value, "All values should be present in the diff file"
                )
