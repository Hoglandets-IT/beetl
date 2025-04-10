import os
import unittest

import polars as pl

from src.beetl import beetl
from tests.configurations.excel import diff_to_xlsx, to_xlsx
from tests.helpers.manual_result import ManualResult
from tests.helpers.temp import TEMP_PATH, clean_temp_directory


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
            self.assertEqual(1, result.height)
        finally:
            clean_temp_directory()
