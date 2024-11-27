import unittest

from bson import ObjectId
import sqlalchemy
from src.beetl import beetl
from testcontainers.mongodb import MongoDbContainer
from testcontainers.mysql import MySqlContainer
from tests.configurations.mongodb import to_mongodb_with_object_id_as_identifier, to_mongodb_with_int_as_identifier, to_mysql_with_object_id_as_identifier
from tests.helpers.manual_result import ManualResult
from faker import Faker

from tests.helpers.mysql_testcontainer import to_connection_string

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
        """
        This test requires transformers to ensure that the object id field is transformed back to an object id on insertion since the mongodb source is automatically converting it to a string on querying.
        """
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

    def test_sync_between_mongodb_and_mysql(self):
        """
        This test is to ensure that data can be synced between MongoDB and a SQL database. Syncing between a document store and a relational database can be challenging due to the differences in data structure. This test will ensure that the beetl library can handle these differences and sync data between the two sources.

        The MongoDB source will be configured with documents looking like this:
        {
            "_id": ObjectId(),
            "name": "John Doe",
            "email": "john@doe.com",
            "children": [{"name": "Jane Doe"}, {"name": "Joseph Doe"}],
            "address": {"city": "New York", "state": "NY"}
        }

        The MySQL source will be configured with a table looking like this:

        +----+----------+-----------------+-----------------+----------------------+
        | id | name     | email           | city            | children             |
        +----+----------+-----------------+-----------------+----------------------+
        | 1  | John Doe | john@doe.com    | New York        | Jane Doe, Joseph Doe |

        There are a few things to note here:
        - The source has a nested document from which we want to extract the city field, this requires a jsonpath transformer to extract the city field from the address field in the source.
        - The sources _id is an ObjectId, which is not supported by MySQL, the mongodb source will transform this to a string but we need to use the rename transformer to match the column name in the MySQL source
        - The children field is an array of documents, we want to extract the name field from each document and concatenate them into a single string separated by a comma. This requires a jsonpath transformer to extract the name field from each child document and a string join transformer to concatenate the names into a single string.

        See the configuration for more details.

        """
        with MongoDbContainer() as mongodb:
            with MySqlContainer("mysql:latest", ) as mysql:
                # Arrange
                engine = sqlalchemy.create_engine(mysql.get_connection_url())
                with engine.begin() as connection:
                    command = f"""
                        create table dst (id varchar({len(str(ObjectId()))}) primary key, name varchar(255), email varchar(255), city varchar(255), children varchar(255));"""
                    connection.execute(sqlalchemy.text(command))

                inserted_ids = self.insert_test_data(3, "object_id", mongodb)
                config = to_mysql_with_object_id_as_identifier(
                    mongodb.get_connection_url(), to_connection_string(mysql.get_connection_url()))
                beetlInstance = beetl.Beetl(beetl.BeetlConfig(config))

                # Act
                createResult = beetlInstance.sync()

                noActionResult = beetlInstance.sync()

                self.update_test_data(
                    inserted_ids[0], self.faker.email(), mongodb)
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
