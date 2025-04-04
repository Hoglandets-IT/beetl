from copy import deepcopy
from unittest import TestCase

import polars as pl

from src.beetl.diff import DiffCalculator
from src.beetl.typings import ComparisonColumn


class DiffCalculatorUnitTests(TestCase):
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

        columns = [
            ComparisonColumn("id", "Int64", True),
            ComparisonColumn("name", "String", False),
            ComparisonColumn("age", "Int64", False),
        ]

        # act
        diff_calculator = DiffCalculator(name, source, destination, columns)
        diff = diff_calculator.create_diff()

        # assert
        self.assertEqual(name, diff.name)

        self.assertEqual(len(diff.inserts), 4)
        insert = diff.inserts[0]
        self.assertEqual(insert, {"id": 1, "name": "test1", "age": 20})

        self.assertEqual(len(diff.updates), 2)
        update = diff.updates[0]
        self.assertEqual(update.old, {"id": 6, "name": "test", "age": 20})
        self.assertEqual(update.new, {"id": 6, "name": "test6", "age": 20})

        self.assertEqual(len(diff.deletes), 1)
        delete = diff.deletes[0]
        self.assertEqual(delete, {"id": 5, "name": "test5", "age": 20})

        self.assertEqual(diff.stats.inserts, 4)
        self.assertEqual(diff.stats.updates, 2)
        self.assertEqual(diff.stats.deletes, 1)

        print(diff.dump_json())
