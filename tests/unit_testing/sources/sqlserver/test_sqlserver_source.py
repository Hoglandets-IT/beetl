from unittest import TestCase
import polars as pl

from src.beetl.sources import SqlserverSource

EMPTY_GUID = "00000000-0000-0000-0000-000000000000"


class UnitTestSqlserverSource(TestCase):
    def _generate_minimum_viable_sut(self) -> SqlserverSource:
        return SqlserverSource(
            {
                "name": "test",
                "connection": {
                    "settings": {
                        "connection_string": "test",
                    }
                },
            }
        )

    def test_is_valid_uuid__when_valid_uuid_string_is_provided__returns_true(self):
        sut = self._generate_minimum_viable_sut()
        result = sut._is_valid_uuid(EMPTY_GUID)
        self.assertTrue(result)

    def test_is_valid_uuid__when_non_uuid_string_is_provided__returns_false(self):
        sut = self._generate_minimum_viable_sut()
        result = sut._is_valid_uuid("test")
        self.assertFalse(result)

    def test_collate_clause__when_uuid_is_provided__does_not_cast_or_collate(self):
        column_name = "id"
        data = pl.DataFrame({"id": [EMPTY_GUID]})
        sut = self._generate_minimum_viable_sut()
        result = sut._collate_clause(column_name, data, "left", "right")
        self.assertTrue("TRY_CONVERT" not in result)
        self.assertTrue("COLLATE" not in result)

    def test_collate_clause__when_non_uuid_string_is_provided__does_not_cast_or_collate(
        self,
    ):
        column_name = "id"
        data = pl.DataFrame({"id": ["1"]})
        sut = self._generate_minimum_viable_sut()
        result = sut._collate_clause(column_name, data, "left", "right")
        self.assertTrue("TRY_CONVERT" in result)
        self.assertTrue("COLLATE" in result)

    def test_collate_clause__when_non_uuid_number_is_provided__does_not_cast_or_collate(
        self,
    ):
        column_name = "id"
        data = pl.DataFrame({"id": [1]})
        sut = self._generate_minimum_viable_sut()
        result = sut._collate_clause(column_name, data, "left", "right")
        self.assertTrue("TRY_CONVERT" not in result)
        self.assertTrue("COLLATE" not in result)
