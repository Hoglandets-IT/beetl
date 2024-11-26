import unittest
import psycopg
from src.beetl import beetl
from testcontainers.mongodb import MongoDbContainer
from tests.configurations import generate_from_mongodb_to_mongodb_no_transformation
from tests.helpers.manual_result import ManualResult

DATABASE_NAME = "test"
SOURCE_TABLE_NAME = "src"


class TestMongodbSource(unittest.TestCase):
    """Basic functionality test for the MongoDB source found in src/beetl/sources/mongodb.py"""

    def insert_test_data(self, mongodb: MongoDbContainer) -> None:
        with mongodb.get_connection_client() as client:
            collection = client[DATABASE_NAME][SOURCE_TABLE_NAME]
            result = collection.insert_many([
                {"id": 1, "name": "John Doe", "email": "john@doe.com",
                    "children": [{"name": "Alice"}, {"name": "Bob"}]},
                {"id": 2, "name": "Jane Doe", "email": "jane@doe.com",
                    "address": {"city": "New York", "state": "NY"}},
                {"id": 3, "name": "Joseph Doe", "email": "joseph@doe.com"}
            ])
            self.assertEqual(len(result.inserted_ids), 3)

    def update_test_data(self, id: int, email: str, mongodb: MongoDbContainer) -> None:
        with mongodb.get_connection_client() as client:
            collection = client[DATABASE_NAME][SOURCE_TABLE_NAME]
            result = collection.update_one(
                {"id": id}, {"$set": {"email": email}})
            self.assertEqual(result.modified_count, 1)

    def delete_test_data(self, id: int, mongodb: MongoDbContainer) -> None:
        with mongodb.get_connection_client() as client:
            collection = client[DATABASE_NAME][SOURCE_TABLE_NAME]
            result = collection.delete_one({"id": id})
            self.assertEqual(result.deleted_count, 1)

    def test_sync_between_two_mongodb_sources(self):
        with MongoDbContainer() as mongodb:
            # Arrange
            self.insert_test_data(mongodb)
            config = generate_from_mongodb_to_mongodb_no_transformation(
                mongodb.get_connection_url())
            beetlInstance = beetl.Beetl(beetl.BeetlConfig(config))

            # Act
            createResult = beetlInstance.sync()

            noActionResult = beetlInstance.sync()

            self.update_test_data(1, 'new@email.com', mongodb)
            updateResult = beetlInstance.sync()

            self.delete_test_data(1, mongodb)
            deleteResult = beetlInstance.sync()

            # Assert
            allEntriesWereSynced = ManualResult(3, 0, 0)
            self.assertEqual(createResult, allEntriesWereSynced)

            nothingChanged = ManualResult(0, 0, 0)
            self.assertEqual(noActionResult, nothingChanged)

            oneRecordWasUpdated = ManualResult(0, 1, 0)
            self.assertEqual(updateResult, oneRecordWasUpdated)

            oneRecordWasDeleted = ManualResult(0, 0, 1)
            self.assertEqual(deleteResult, oneRecordWasDeleted)
