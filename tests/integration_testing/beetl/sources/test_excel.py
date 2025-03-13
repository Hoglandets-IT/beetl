import unittest
from src.beetl import beetl
from tests.configurations.excel import to_xlsx
from tests.helpers.manual_result import ManualResult


class TestExcelSource(unittest.TestCase):
    """Basic test of the static source found in src/beetl/sources/static.py"""

    def test_xlsx_loading(self):
        # Arrange
        betl = beetl.Beetl(beetl.BeetlConfig(to_xlsx("tests/fake-data/datafiles/src.xlsx", "tests/fake-data/datafiles/dst.xlsx")))

        # Act
        amounts = betl.sync()

        # # Assert
        self.assertEqual(
            amounts,
            ManualResult(91, 7, 8)
        )
