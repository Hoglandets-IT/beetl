import unittest
import polars as pl
from src.beetl.sources.excel.excel_source import ExcelSource
from src.beetl import beetl
from tests.configurations.excel import to_xlsx
from tests.helpers.manual_result import ManualResult


class TestExcelSource(unittest.TestCase):
    """Basic test of the static source found in src/beetl/sources/static.py"""

    def test_xlsx_loading(self):
        # Arrange
        betl = beetl.Beetl(
            beetl.BeetlConfig(
                to_xlsx(
                    "tests/fake-data/datafiles/src.xlsx",
                    "tests/fake-data/datafiles/dst.xlsx",
                )
            )
        )

        # Act
        amounts = betl.sync()

        # # Assert
        self.assertEqual(amounts, ManualResult(91, 7, 8))

    def test_query__with_override_columns__types_should_be_overridden(self):
        # Arrange
        source: ExcelSource = ExcelSource(
            {
                "connection": {
                    "path": "tests/fake-data/datafiles/test_override_columns.xlsx"
                },
                "name": "Name",
            }
        )
        source.set_sourceconfig({"types": {"2": "Int64"}}, "source", "name", [])

        # Act
        data = source._query()

        # # Assert
        self.assertIsInstance(data.schema["2"], pl.Int64)
