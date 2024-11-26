import unittest

from bson import ObjectId
from src.beetl import beetl
from testcontainers.mongodb import MongoDbContainer
from tests.configurations.mongodb import to_mongodb_with_object_id_as_identifier, to_mongodb_with_int_as_identifier
from tests.helpers.manual_result import ManualResult
from faker import Faker

DATABASE_NAME = "test"
SOURCE_TABLE_NAME = "src"


class TestMongodbSource(unittest.TestCase):
    """Basic functionality test for the MongoDB source found in src/beetl/sources/mongodb.py"""
    faker = Faker()

    def insert_test_data(self, amount: int, id_type: str, mongodb: MongoDbContainer) -> list:
        inserts = []
        for _ in range(amount):
            inserts.append({
                "_id": ObjectId() if id_type == "object_id" else self.faker.random_int(),
                "name": self.faker.name(),
                "email": self.faker.email(),
                "children": [{"name": self.faker.first_name()}, {"name": self.faker.first_name()}],
                "address": {"city": self.faker.city(), "state": self.faker.state()}
            })

        with mongodb.get_connection_client() as client:
            collection = client[DATABASE_NAME][SOURCE_TABLE_NAME]
            result = collection.insert_many(inserts)
            self.assertEqual(len(result.inserted_ids), len(inserts))
            return result.inserted_ids

    def update_test_data(self, id: str | ObjectId, email: str, mongodb: MongoDbContainer) -> None:
        with mongodb.get_connection_client() as client:
            collection = client[DATABASE_NAME][SOURCE_TABLE_NAME]
            result = collection.update_one(
                {"_id": id}, {"$set": {"email": email}})
            self.assertEqual(result.modified_count, 1)

    def delete_test_data(self, id: str | ObjectId, mongodb: MongoDbContainer) -> None:
        with mongodb.get_connection_client() as client:
            collection = client[DATABASE_NAME][SOURCE_TABLE_NAME]
            result = collection.delete_one({"_id": id})
            self.assertEqual(result.deleted_count, 1)

    def test_sync_between_two_mongodb_sources_with_object_id(self):
        with MongoDbContainer() as mongodb:
            # Arrange
            inserted_ids = self.insert_test_data(3, "object_id", mongodb)
            config = to_mongodb_with_object_id_as_identifier(
                mongodb.get_connection_url())
            beetlInstance = beetl.Beetl(beetl.BeetlConfig(config))

            # Act
            createResult = beetlInstance.sync()

            noActionResult = beetlInstance.sync()

            self.update_test_data(inserted_ids[0], self.faker.email(), mongodb)
            updateResult = beetlInstance.sync()

            self.delete_test_data(inserted_ids[0], mongodb)
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

    def test_sync_between_two_mongodb_sources_with_int_id(self):
        with MongoDbContainer() as mongodb:
            # Arrange
            inserted_ids = self.insert_test_data(3, "int", mongodb)
            config = to_mongodb_with_int_as_identifier(
                mongodb.get_connection_url())
            beetlInstance = beetl.Beetl(beetl.BeetlConfig(config))

            # Act
            createResult = beetlInstance.sync()

            noActionResult = beetlInstance.sync()

            self.update_test_data(inserted_ids[0], self.faker.email(), mongodb)
            updateResult = beetlInstance.sync()

            self.delete_test_data(inserted_ids[0], mongodb)
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
