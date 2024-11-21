import unittest
import psycopg
from src.beetl import beetl
from testcontainers.postgres import PostgresContainer
from tests.helpers.sync_result import create_sync_result


class TestMysqlSource(unittest.TestCase):
    """Basic functionality test for the PostgreSQl source found in src/beetl/sources/postgresql.py"""

    @classmethod
    def setUpClass(self):
        pass

    def buildConfig(self, connectionString: str):
        if (not connectionString):
            raise Exception("Connection string is required")

        return {
            "version": "V1",
            "sources": [
                {
                    "name": "database",
                    "type": "Postgresql",
                    "connection": {
                        "settings": {
                            "connection_string": connectionString
                        }

                    }
                }
            ],
            "sync": [
                {
                    "source": "database",
                    "destination": "database",
                    "sourceConfig": {
                        "table": "srctable",
                        "columns": [
                            {
                                "name": "id",
                                "type": "Int32",
                                "unique": True,
                                "skip_update": True
                            },
                            {
                                "name": "name",
                                "type": "Utf8",
                                "unique": False,
                                "skip_update": False
                            },
                            {
                                "name": "email",
                                "type": "Utf8",
                                "unique": False,
                                "skip_update": False
                            }
                        ]
                    },
                    "destinationConfig": {
                        "table": "dsttable",
                        "columns": [
                            {
                                "name": "id",
                                "type": "Int32",
                                "unique": True,
                                "skip_update": True
                            },
                            {
                                "name": "name",
                                "type": "Utf8",
                                "unique": False,
                                "skip_update": False
                            },
                            {
                                "name": "email",
                                "type": "Utf8",
                                "unique": False,
                                "skip_update": False
                            }
                        ]
                    }
                }
            ]
        }

    def insert_test_data(self, postgresql: PostgresContainer) -> None:
        connection_url = postgresql.get_connection_url()
        with psycopg.connect(connection_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "create table srctable (id int primary key, name varchar(255), email varchar(255))")
                cursor.execute(
                    "create table dsttable (id int primary key, name varchar(255), email varchar(255))")
                cursor.execute(
                    "insert into srctable (id, name, email) values (1, 'John Doe', 'john@doe.com'),(2, 'Jane Doe', 'jane@doe.com'),(3, 'Joseph Doe', 'joseph@doe.com')")

    def update_test_data(self, id: int, email: str, postgresql: PostgresContainer) -> None:
        connection_url = postgresql.get_connection_url()
        with psycopg.connect(connection_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"update srctable set email = '{email}' where id = {id}")

    def delete_test_data(self, id: int, postgresql: PostgresContainer) -> None:
        connection_url = postgresql.get_connection_url()
        with psycopg.connect(connection_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"delete from srctable where id = {id}")

    def test_sync_between_two_postgresql_sources(self):
        with PostgresContainer(driver=None) as postgresql:
            # Arrange
            self.insert_test_data(postgresql)
            config = self.buildConfig(postgresql.get_connection_url())
            beetlInstance = beetl.Beetl(beetl.BeetlConfig(config))

            # Act
            createResult = beetlInstance.sync()

            noActionResult = beetlInstance.sync()

            self.update_test_data(1, 'new@email.com', postgresql)
            updateResult = beetlInstance.sync()

            self.delete_test_data(1, postgresql)
            deleteResult = beetlInstance.sync()

            # Assert
            allEntriesWereSynced = create_sync_result(3, 0, 0)
            self.assertEqual(createResult, allEntriesWereSynced)

            nothingChanged = create_sync_result(0, 0, 0)
            self.assertEqual(noActionResult, nothingChanged)

            oneRecordWasUpdated = create_sync_result(0, 1, 0)
            self.assertEqual(updateResult, oneRecordWasUpdated)

            oneRecordWasDeleted = create_sync_result(0, 0, 1)
            self.assertEqual(deleteResult, oneRecordWasDeleted)
