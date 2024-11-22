import unittest
import psycopg
from src.beetl import beetl
from testcontainers.mongodb import MongoDbContainer
from tests.configurations import generate_from_postgres_to_postgres
from tests.helpers.manual_result import ManualResult


class TestMongodbSource(unittest.TestCase):
    """Basic functionality test for the MongoDB source found in src/beetl/sources/mongodb.py"""

    def insert_test_data(self, mongodb: MongoDbContainer) -> None:
        db = mongodb.get_connection_client().test
        db.srctable.insert_many([
            {"id": 1, "name": "John Doe", "email": "john@doe.com"},
            {"id": 2, "name": "Jane Doe", "email": "jane@doe.com"},
            {"id": 3, "name": "Joseph Doe", "email": "joseph@doe.com"}
        ])

    # TODO: Adjust for mongodb

    def update_test_data(self, id: int, email: str, postgresql: MongoDbContainer) -> None:
        connection_url = postgresql.get_connection_url()
        with psycopg.connect(connection_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"update srctable set email = '{email}' where id = {id}")

    # TODO: Adjust for mongodb
    def delete_test_data(self, id: int, postgresql: MongoDbContainer) -> None:
        connection_url = postgresql.get_connection_url()
        with psycopg.connect(connection_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"delete from srctable where id = {id}")

    # TODO: Adjust for mongodb
    def test_sync_between_two_mongodb_sources(self):
        with MongoDbContainer() as mongodb:
            # Arrange
            self.insert_test_data(mongodb)
            config = generate_from_postgres_to_postgres(
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
