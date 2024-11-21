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
                    "name": "postgresqlsrc",
                    "type": "PostgreSQL",
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
                    "source": "postgresqlsrc",
                    "destination": "postgresqldst",
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

    def insert_test_data(self, postgress: PostgresContainer) -> None:
        connection_url = postgress.get_connection_url()
        with psycopg.connect(connection_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "create table srctable (id int primary key, name varchar(255), email varchar(255))")
                cursor.execute(
                    "create table dsttable (id int primary key, name varchar(255), email varchar(255))")
                cursor.execute(
                    "insert into srctable (id, name, email) values (1, 'John Doe', 'john@doe.com'),(2, 'Jane Doe', 'jane@doe.com'),(3, 'Joseph Doe', 'joseph@doe.com')")

    def test_sync_between_two_postgresql_sources(self):
        with PostgresContainer(driver=None) as postgress:
            # Arrange
            self.insert_test_data(postgress)
            connection_string = postgress.get_connection_url()
            config = self.buildConfig(connection_string)
            beetlInstance = beetl.Beetl(beetl.BeetlConfig(config))

            # Act
            firstSyncResults = beetlInstance.sync()
            secondSyncResults = beetlInstance.sync()

            # Assert
            allEntriesWereSynced = create_sync_result(3, 0, 0)
            self.assertEqual(firstSyncResults, allEntriesWereSynced)

            nothingChanged = create_sync_result(0, 0, 0)
            self.assertEqual(secondSyncResults, nothingChanged)
