import os
import unittest

import polars as pl

from src.beetl import beetl
from src.beetl.sources.excel.excel_source import ExcelSource
from src.beetl.comparison_result import ComparisonResult
from tests.configurations.excel import diff_to_xlsx, to_xlsx, to_xlsx_with_sheet_name
from tests.helpers.manual_result import ManualResult
from tests.helpers.temp import TEMP_PATH, clean_temp_directory


class TestExcelSource(unittest.TestCase):
    """Basic test of the static source found in src/beetl/sources/static.py

    The tests in where work since dst is never updated.
    That is because using excel as a destination isn't implemented yet.
    """

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

    def test_excel_with_multiple_workbooks__when_workbook_is_specified__loads_that_workbook(
        self,
    ):
        # Arrange
        betl = beetl.Beetl(
            beetl.BeetlConfig(
                to_xlsx_with_sheet_name(
                    "tests/fake-data/datafiles/src_with_multiple_workbooks.xlsx",
                    "tests/fake-data/datafiles/dst.xlsx",
                )
            )
        )

        # Act
        dry_run: list[ComparisonResult] = betl.sync(dry_run=True)
        changes = dry_run[0]
        result = changes.to_beetl_result()

        # # Assert
        self.assertEqual(result, ManualResult(91, 7, 8))

    def test_save_diff__when_configured__diff_is_saved_to_temp_dir(self):
        clean_temp_directory()
        try:
            # Arrange

            diff_file_path = os.path.join(TEMP_PATH, "diff.xlsx")
            beetl_instance = beetl.Beetl(
                beetl.BeetlConfig(diff_to_xlsx(diff_file_path))
            )

            # Act
            beetl_instance.sync()

            # # Assert
            self.assertTrue(os.path.exists(diff_file_path))

            result = pl.read_excel(diff_file_path)
            self.assertGreater(result.height, 0)
            for row in result.to_dicts():
                self.assertIsNotNone(row)
                for key, value in row.items():
                    self.assertIsNotNone(value, f"Value for key '{key}' is None")

        finally:
            clean_temp_directory()

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
        # OK to access the private method since we are testing the very method.
        # pylint: disable=protected-access
        data = source._query()

        # # Assert
        self.assertIsInstance(data.schema["2"], pl.Int64)
