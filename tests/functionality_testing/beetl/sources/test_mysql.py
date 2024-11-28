import unittest
import sqlalchemy
from src.beetl import beetl
from testcontainers.mysql import MySqlContainer
from tests.configurations.mysql import to_mongodb_with_object_id_as_identifier
from tests.helpers.manual_result import ManualResult
from tests.helpers.mysql_testcontainer import to_connection_string
from testcontainers.mongodb import MongoDbContainer


class TestMysqlSource(unittest.TestCase):
    """Basic functionality test for the MySQL source found in src/beetl/sources/mysql.py"""

    def setUp(self):
        pass

    def buildConfig(self, connectionString: str):
        if (not connectionString):
            raise Exception("Connection string is required")

        return {
            "version": "V1",
            "sources": [
                {
                    "name": "mysqlsrc",
                    "type": "Mysql",
                    "connection": {
                        "settings": {
                            "connection_string": connectionString
                        }

                    }
                },
                {
                    "name": "mysqldst",
                    "type": "Mysql",
                    "connection": {
                        "settings": {
                            "connection_string": connectionString
                        }
                    }
                }
            ],
            "sync": [
                {
                    "source": "mysqlsrc",
                    "destination": "mysqldst",
                    "sourceConfig": {
                        "table": "srctable",
                        "columns": [
                            {
                                "name": "id",
                                "type": "Utf8",
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
                                "type": "Utf8",
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
                    "destinationTransformers": [
                        {
                            "transformer": "strings.lowercase",
                            "config": {
                                "inField": "name",
                                "outField": "nameLower"
                            }
                        },
                        {
                            "transformer": "strings.uppercase",
                            "config": {
                                "inField": "name",
                                "outField": "nameUpper"
                            }
                        },
                        {
                            "transformer": "strings.split",
                            "config": {
                                "inField": "email",
                                "outFields": [
                                    "username",
                                    "domain"
                                ],
                                "separator": "@"
                            }
                        },
                        {
                            "transformer": "strings.join",
                            "config": {
                                "inFields": [
                                    "nameLower",
                                    "nameUpper"
                                ],
                                "outField": "displayName",
                                "separator": " ^-^ "
                            }
                        },
                        {
                            "transformer": "frames.drop_columns",
                            "config": {
                                "columns": [
                                    "nameLower",
                                    "nameUpper"
                                ]
                            }
                        }
                    ]
                }
            ]
        }

    def insert_test_data(self, mysql: MySqlContainer) -> None:
        engine = sqlalchemy.create_engine(mysql.get_connection_url())
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("create table srctable (id int primary key, name varchar(255), email varchar(255), children varchar(255), city varchar(255))"))
            connection.execute(
                sqlalchemy.text("create table dsttable (id int primary key, name varchar(255), email varchar(255), children varchar(255), city varchar(255))"))
            connection.execute(sqlalchemy.text(
                "insert into srctable (id, name, email, children, city) values (1, 'John Doe', 'john@doe.com', 'Joseph,Annie', 'New York'),(2, 'Jane Doe', 'jane@doe.com', 'Jacob,Billy', 'Stockholm'),(3, 'Joseph Doe', 'joseph@doe.com', 'Hannah,Jack', 'Oslo')"))

    def update_test_data(self, id: int, email: str, mysql: MySqlContainer) -> None:
        engine = sqlalchemy.create_engine(mysql.get_connection_url())
        with engine.begin() as connection:
            connection.execute(sqlalchemy.text(
                "update srctable set email = :email where id = :id"), {"email": email, "id": id})

    def remove_test_data(self, id: int, mysql: MySqlContainer) -> None:
        engine = sqlalchemy.create_engine(mysql.get_connection_url())
        with engine.begin() as connection:
            connection.execute(sqlalchemy.text(
                "delete from srctable where id = :id"), {"id": id})

    def test_sync_between_two_mysql_sources(self):
        with MySqlContainer("mysql:latest") as mysql:
            # Arrange
            self.insert_test_data(mysql)
            connection_string = to_connection_string(
                mysql.get_connection_url())
            config = self.buildConfig(connection_string)
            beetlInstance = beetl.Beetl(beetl.BeetlConfig(config))

            # Act
            createdResults = beetlInstance.sync()
            nothingChangedResults = beetlInstance.sync()

            self.update_test_data(1, "new@email.com", mysql)
            updateResults = beetlInstance.sync()

            self.remove_test_data(1, mysql)
            deleteResults = beetlInstance.sync()

            # Assert
            allEntriesWereSynced = ManualResult(3, 0, 0)
            self.assertEqual(createdResults, allEntriesWereSynced)

            nothingChanged = ManualResult(0, 0, 0)
            self.assertEqual(nothingChangedResults, nothingChanged)

            oneEntryUpdated = ManualResult(0, 1, 0)
            self.assertEqual(updateResults, oneEntryUpdated)

            oneEntryDeleted = ManualResult(0, 0, 1)
            self.assertEqual(deleteResults, oneEntryDeleted)

    def test_sync_between_mysql_and_mongodb(self):
        with MySqlContainer() as mysql:
            with MongoDbContainer() as mongo:
                # Arrange
                self.insert_test_data(mysql)
                config = to_mongodb_with_object_id_as_identifier(to_connection_string(
                    mysql.get_connection_url()), mongo.get_connection_url())
                beetlInstance = beetl.Beetl(beetl.BeetlConfig(config))

                # Act
                createdResults = beetlInstance.sync()
                nothingChangedResults = beetlInstance.sync()

                self.update_test_data(1, "new@email.com", mysql)
                updateResults = beetlInstance.sync()

                self.remove_test_data(1, mysql)
                deleteResults = beetlInstance.sync()

                # Assert
                allEntriesWereSynced = ManualResult(3, 0, 0)
                self.assertEqual(createdResults, allEntriesWereSynced)

                nothingChanged = ManualResult(0, 0, 0)
                self.assertEqual(nothingChangedResults, nothingChanged)

                oneEntryUpdated = ManualResult(0, 1, 0)
                self.assertEqual(updateResults, oneEntryUpdated)

                oneEntryDeleted = ManualResult(0, 0, 1)
                self.assertEqual(deleteResults, oneEntryDeleted)
