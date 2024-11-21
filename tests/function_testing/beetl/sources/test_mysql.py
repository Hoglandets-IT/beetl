import unittest
from src.beetl import beetl
from testcontainers.mysql import MySqlContainer
from tests.helpers.sync_result import create_sync_result


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
                    "config": {
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
                    "connection": {
                        "settings": {
                            "connection_string": connectionString
                        }

                    }
                },
                {
                    "name": "mysqldst",
                    "type": "Mysql",
                    "config": {
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
                    "fieldTransformers": [
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

    def test_benchmark_mysql(self):
        with MySqlContainer("mysql:latest") as mysql:
            connection_string = mysql.get_connection_url()
            config = self.buildConfig(connection_string)
            betl = beetl.Beetl(beetl.BeetlConfig(config))
            amounts = betl.sync()

        print(amounts)

    def test_mysql(self):
        betl = beetl.Beetl(beetl.BeetlConfig(self.basicConfig))
        amounts = betl.sync()

        self.assertEqual(
            amounts,
            create_sync_result(1, 1, 1)
        )

        # When running again, the result should be 0, 0, 0
        amountsTwo = beetl.sync()
        self.assertEqual(
            amountsTwo,
            create_sync_result(0, 0, 0)
        )
