import unittest
import psycopg
from src.beetl import beetl
from testcontainers.postgres import PostgresContainer
from tests.configurations import generate_from_postgres_to_postgres
from tests.helpers.manual_result import ManualResult


class TestPostgresqlSource(unittest.TestCase):
    """Basic functionality test for the PostgreSQl source found in src/beetl/sources/postgresql.py"""

    @classmethod
    def setUpClass(self):
        pass

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
            config = generate_from_postgres_to_postgres(
                postgresql.get_connection_url())
            beetlInstance = beetl.Beetl(beetl.BeetlConfig(config))

            # Act
            createResult = beetlInstance.sync()

            noActionResult = beetlInstance.sync()

            self.update_test_data(1, 'new@email.com', postgresql)
            updateResult = beetlInstance.sync()

            self.delete_test_data(1, postgresql)
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
