import unittest
import sqlalchemy
from testcontainers.mssql import SqlServerContainer

from src.beetl import beetl
from tests.configurations.sqlserver import to_sqlserver
from tests.helpers.manual_result import ManualResult
from tests.helpers.sqlserver_testcontainer import to_connection_string


class TestSqlServerSource(unittest.TestCase):
    """Basic functionality test for the SqlServer source found in src/beetl/sources/sqlserver.py"""

    def insert_test_data(self, mssql: SqlServerContainer) -> None:
        engine = sqlalchemy.create_engine(mssql.get_connection_url())
        with engine.begin() as connection:

            connection.execute(
                sqlalchemy.text(
                    "create table src (id int primary key, name varchar(255), email varchar(255))"
                )
            )
            connection.execute(
                sqlalchemy.text(
                    "create table dst (id int primary key, name varchar(255), email varchar(255))"
                )
            )
            connection.execute(
                sqlalchemy.text(
                    "insert into src (id, name, email) values (1, 'John Doe', 'john@doe.com'),(2, 'Jane Doe', 'jane@doe.com'),(3, 'Joseph Doe', 'joseph@doe.com')"
                )
            )

    def update_test_data(self, id: int, email: str, mssql: SqlServerContainer) -> None:
        engine = sqlalchemy.create_engine(mssql.get_connection_url())
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.text(f"update src set email = '{email}' where id = {id}")
            )

    def delete_test_data(self, id: int, mssql: SqlServerContainer) -> None:
        engine = sqlalchemy.create_engine(mssql.get_connection_url())
        with engine.begin() as connection:
            connection.execute(sqlalchemy.text(f"delete from src where id = {id}"))

    def test_sync_between_two_sqlserver_sources(self):
        with SqlServerContainer() as mssql:
            # Arrange
            self.insert_test_data(mssql)
            connection_string = to_connection_string(mssql.get_connection_url())
            config = to_sqlserver(connection_string)
            beetlInstance = beetl.Beetl(beetl.BeetlConfig(config))

            # Act
            createResult = beetlInstance.sync()

            noActionResult = beetlInstance.sync()

            self.update_test_data(1, "new@email.com", mssql)
            updateResult = beetlInstance.sync()

            self.delete_test_data(1, mssql)
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
