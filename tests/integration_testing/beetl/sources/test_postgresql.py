import unittest

import psycopg
from testcontainers.postgres import PostgresContainer

from src.beetl import beetl
from tests.configurations.postgresql import diff_to_postgres, to_postgres
from tests.helpers.manual_result import ManualResult


class TestPostgresqlSource(unittest.TestCase):
    """Basic functionality test for the PostgreSQl source"""

    def _insert_test_data(self, postgresql: PostgresContainer) -> None:
        connection_url = postgresql.get_connection_url()
        # Pylint failes to acnowledge that the context manager is used
        # The changes won't commit if you remove it
        # pylint: disable=not-context-manager
        with psycopg.connect(connection_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "create table srctable (id int primary key, name varchar(255), email varchar(255))"
                )
                cursor.execute(
                    "create table dsttable (id int primary key, name varchar(255), email varchar(255))"
                )
                cursor.execute(
                    "insert into srctable (id, name, email) values (1, 'John Doe', 'john@doe.com'),(2, 'Jane Doe', 'jane@doe.com'),(3, 'Joseph Doe', 'joseph@doe.com')"
                )

    def _update_test_data(
        self, identifier: int, email: str, postgresql: PostgresContainer
    ) -> None:
        connection_url = postgresql.get_connection_url()
        # Pylint failes to acnowledge that the context manager is used
        # The changes won't commit if you remove it
        # pylint: disable=not-context-manager
        with psycopg.connect(connection_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"update srctable set email = '{email}' where id = {identifier}"
                )

    def _delete_test_data(self, identifier: int, postgresql: PostgresContainer) -> None:
        connection_url = postgresql.get_connection_url()
        # Pylint failes to acnowledge that the context manager is used
        # The changes won't commit if you remove it
        # pylint: disable=not-context-manager
        with psycopg.connect(connection_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"delete from srctable where id = {identifier}")

    def test_store_diff__when_diff_is_configured__diff_is_stored_in_database(self):
        with PostgresContainer(driver=None) as postgresql:
            # Arrange
            config = diff_to_postgres(postgresql.get_connection_url())
            beetl_instance = beetl.Beetl(beetl.BeetlConfig(config))

            # Act
            beetl_instance.sync()

            # Assert
            connection = psycopg.connect(postgresql.get_connection_url())
            with connection.cursor() as cursor:
                result = list(cursor.execute("select * from diff"))
                for row in result:
                    for column in row:
                        self.assertIsNotNone(column)

    def test_sync_between_two_postgresql_sources(self):
        with PostgresContainer(driver=None) as postgresql:
            # Arrange
            self._insert_test_data(postgresql)
            config = to_postgres(postgresql.get_connection_url())
            beetl_instance = beetl.Beetl(beetl.BeetlConfig(config))

            # Act
            create_result = beetl_instance.sync()

            no_action_result = beetl_instance.sync()

            self._update_test_data(1, "new@email.com", postgresql)
            update_result = beetl_instance.sync()

            self._delete_test_data(1, postgresql)
            delete_result = beetl_instance.sync()

            # Assert
            all_entries_were_synced = ManualResult(3, 0, 0)
            self.assertEqual(create_result, all_entries_were_synced)

            nothing_changed = ManualResult(0, 0, 0)
            self.assertEqual(no_action_result, nothing_changed)

            one_record_was_updated = ManualResult(0, 1, 0)
            self.assertEqual(update_result, one_record_was_updated)

            one_record_was_deleted = ManualResult(0, 0, 1)
            self.assertEqual(delete_result, one_record_was_deleted)
