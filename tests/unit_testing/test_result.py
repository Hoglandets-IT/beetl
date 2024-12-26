
import unittest
from src.beetl.result import SyncResult


class TestResult(unittest.TestCase):

    def test_str__with_one_insert_two_updates_three_deletes_one_sync__returns_properly_formatted_string(self):
        expected = "Result: 1 inserts, 2 updates, 3 deletes across 1 syncs"
        result = SyncResult([("Sync One", 1, 2, 3)])
        self.assertEqual(expected, result.__str__())

    def test_repr__with_one_insert_two_updates_three_deletes_one_sync__returns_properly_formatted_string(self):
        expected = "Result: 1 inserts, 2 updates, 3 deletes across 1 syncs"
        result = SyncResult([("Sync One", 1, 2, 3)])
        self.assertEqual(expected, result.__repr__())

    def test_eq__with_same_inserts_updates_deletes__returns_true(self):
        result1 = SyncResult([("Sync One", 1, 2, 3)])
        result2 = SyncResult([("Sync One", 1, 2, 3)])
        self.assertEqual(result1, result2)

    def test_eq__with_different_inserts__returns_false(self):
        result1 = SyncResult([("Sync One", 1, 2, 3)])
        result2 = SyncResult([("Sync One", 2, 2, 3)])
        self.assertNotEqual(result1, result2)

    def test_eq__with_different_updates__returns_false(self):
        result1 = SyncResult([("Sync One", 1, 2, 3)])
        result2 = SyncResult([("Sync One", 1, 3, 3)])
        self.assertNotEqual(result1, result2)

    def test_eq__with_different_deletes__returns_false(self):
        result1 = SyncResult([("Sync One", 1, 2, 3)])
        result2 = SyncResult([("Sync One", 1, 2, 4)])
        self.assertNotEqual(result1, result2)

    def test_eq__with_different_sync_names__returns_true(self):
        result1 = SyncResult([("Sync One", 1, 2, 3)])
        result2 = SyncResult([("Sync Two", 1, 2, 3)])
        self.assertEqual(result1, result2)

    def test_sync_result_init__list_of_sync_tuples__properly_calculates_inserts_updates_deletes(self):
        result = SyncResult([("Sync One", 1, 2, 3), ("Sync Two", 4, 5, 6)])
        self.assertEqual(5, result.inserts)
        self.assertEqual(7, result.updates)
        self.assertEqual(9, result.deletes)
        self.assertEqual(("Sync One", "Sync Two"), result.names)
