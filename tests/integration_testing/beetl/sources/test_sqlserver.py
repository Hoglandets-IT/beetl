import datetime
import json
import unittest

import sqlalchemy
from testcontainers.mssql import SqlServerContainer

from src.beetl import beetl
from tests.configurations.sqlserver import (
    from_static_to_sqlserver_with_diff,
    to_sqlserver,
)
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

    def test_diff_insert_into_sqlserver(self):
        """
        Test that the diff is inserted into the sql database and that the values match what was synced.

        Id 1 will be deleted.

        Id 2 will be created.

        Id 3 will have its name updated.

        Id 4 will have its age updated

        Id 5 will be not have any changes
        """
        with SqlServerContainer() as mssql:
            # Arrange
            connection_string = to_connection_string(mssql.get_connection_url())

            engine = sqlalchemy.create_engine(mssql.get_connection_url())
            with engine.begin() as connection:

                connection.execute(
                    sqlalchemy.text(
                        "create table diffs (uuid uniqueidentifier primary key, name varchar(255), date datetime, version varchar(64), updates nvarchar(max), inserts nvarchar(max), deletes nvarchar(max), stats nvarchar(max))"
                    )
                )
                connection.execute(
                    sqlalchemy.text(
                        "create table dst (id int primary key, name varchar(255), age int)"
                    )
                )
                connection.execute(
                    sqlalchemy.text(
                        "insert into dst (id, name, age) values (1, 'test1', 20),(3, 'test', 20),(4, 'test4', 21), (5, 'test5', 20)"
                    )
                )

            config = from_static_to_sqlserver_with_diff(connection_string)
            beetlInstance = beetl.Beetl(beetl.BeetlConfig(config))

            # Act
            beetlInstance.sync()

            engine = sqlalchemy.create_engine(mssql.get_connection_url())
            with engine.begin() as connection:
                result = connection.execute(
                    sqlalchemy.text("select * from diffs")
                ).fetchall()
            self.assertEqual(1, len(result))
            diff = result[0]
            uuid = str(diff[0])
            name = diff[1]
            date = diff[2].strftime("%Y-%m-%d %H:%M:%S")
            version = diff[3]
            updates = json.loads(diff[4])
            inserts = json.loads(diff[5])
            deletes = json.loads(diff[6])
            stats = json.loads(diff[7])

            self.assertEqual("diff_test", name)
            self.assertEqual("1.0.0", version)
            self.assertRegex(date, f"{datetime.datetime.now().strftime('%Y-%m-%d')}")
            self.assertIsNotNone(uuid)

            self.assertEqual(2, len(updates))
            self.assertEqual(1, len(inserts))
            self.assertEqual(1, len(deletes))

            self.assertEqual(1, stats["inserts"])
            self.assertEqual(2, stats["updates"])
            self.assertEqual(1, stats["deletes"])

            print(diff)
