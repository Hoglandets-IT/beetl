from copy import deepcopy
from unittest import TestCase

import polars as pl

from src.beetl.diff import DiffCalculator


class DiffFactoryUnitTests(TestCase):
    def test_create_diff__when_passed_known_good_values__returns_diff_instance(self):
        # arrange
        name = "name"

        original_data = {
            "id": [1, 2, 3, 4, 6, 7],
            "name": ["test1", "test2", "test3", "test4", "test6", "test7"],
            "age": [20, 20, 20, 20, 20, 20],
        }
        source = pl.DataFrame(original_data)

        destination_data = {
            "id": [5, 6, 7],
            "name": ["test5", "test", "test"],
            "age": [20, 20, 21],
        }
        destination = pl.DataFrame(destination_data)

        unique_columns = ("id",)
        comparison_columns = ("name", "age")

        # act
        diff_calculator = DiffCalculator(
            name, source, destination, unique_columns, comparison_columns
        )
        diff = diff_calculator.create_diff()

        # assert
        self.assertEqual(name, diff.name)

        self.assertEqual(4, len(diff.inserts))
        insert = diff.inserts[0]
        self.assertEqual({"id": 1}, insert.identifiers)
        self.assertEqual({"name": "test1", "age": 20}, insert.data)

        self.assertEqual(2, len(diff.updates))
        update = diff.updates[0]
        self.assertEqual({"id": 6}, update.identifiers)
        self.assertEqual({"name": "test", "age": None}, update.old)
        self.assertEqual({"name": "test6", "age": None}, update.new)

        self.assertEqual(1, len(diff.deletes))
        delete = diff.deletes[0]
        self.assertEqual({"id": 5}, delete.identifiers)

        self.assertEqual(4, diff.stats.inserts)
        self.assertEqual(2, diff.stats.updates)
        self.assertEqual(1, diff.stats.deletes)
        self.assertEqual(("name", "age"), diff.stats.updated_fields)

        print(diff.dump_json())
