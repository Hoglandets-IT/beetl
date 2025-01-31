import unittest

from polars import Int32, Utf8

from src.beetl.beetl import BeetlConfig


class UnitTestBeetlConfig(unittest.TestCase):

    def config_with_columns_as_list(self):
        return {
            "version": "V1",
            "sources": [
                {
                    "name": "src",
                    "type": "Static",
                    "connection": {"static": [{"id": 1, "name": "foo"}]},
                }
            ],
            "sync": [
                {
                    "source": "src",
                    "sourceConfig": {},
                    "destination": "src",
                    "destinationConfig": {},
                    "comparisonColumns": [
                        {"name": "id", "type": "Int32", "unique": True},
                        {"name": "name", "type": "Utf8"},
                    ],
                }
            ],
        }

    def test_config_init__comparison_columns_as_list__fields_are_propagated(self):
        result = BeetlConfig(self.config_with_columns_as_list())

        id = result.sync_list[0].comparisonColumns[0]
        self.assertEqual(id.name, "id")
        self.assertEqual(id.type, Int32)
        self.assertTrue(id.unique)

    def test_config_init__comparison_columns_as_list__fields_without_unique_defaults_to_false(
        self,
    ):
        result = BeetlConfig(self.config_with_columns_as_list())

        id = result.sync_list[0].comparisonColumns[1]
        self.assertEqual(id.name, "name")
        self.assertEqual(id.type, Utf8)
        self.assertFalse(id.unique)

    def config_with_columns_as_dict(self):
        return {
            "version": "V1",
            "sources": [
                {
                    "name": "src",
                    "type": "Static",
                    "connection": {"static": [{"id": 1, "name": "foo"}]},
                }
            ],
            "sync": [
                {
                    "source": "src",
                    "sourceConfig": {},
                    "destination": "src",
                    "destinationConfig": {},
                    "comparisonColumns": {"id": "Int32", "name": "Utf8"},
                }
            ],
        }

    def test_config_init__comparision_columns_as_dict__first_field_is_unique(self):
        result = BeetlConfig(self.config_with_columns_as_dict())
        id = result.sync_list[0].comparisonColumns[0]
        self.assertEqual(id.name, "id")
        self.assertEqual(id.type, Int32)
        self.assertTrue(id.unique)

    def test_config_init__comparision_columns_as_dict__second_field_is_not_unique(self):
        result = BeetlConfig(self.config_with_columns_as_dict())
        name = result.sync_list[0].comparisonColumns[1]
        self.assertEqual(name.name, "name")
        self.assertEqual(name.type, Utf8)
        self.assertFalse(name.unique)

    def test_that_version_1_supports_static_source(self):
        result = BeetlConfig(
            {
                "version": "V1",
                "sources": [
                    {
                        "name": "staticsrc",
                        "type": "Static",
                        "connection": {
                            "static": [
                                {"id": 1, "name": "John", "email": "john@test.com"},
                            ],
                        },
                    },
                    {
                        "name": "staticdst",
                        "type": "Static",
                        "connection": {
                            "static": [
                                {"id": 1, "name": "John", "email": "john@test.com"},
                            ]
                        },
                    },
                ],
                "sync": [
                    {
                        "source": "staticsrc",
                        "destination": "staticdst",
                        "sourceConfig": {},
                        "destinationConfig": {},
                        "comparisonColumns": [
                            {
                                "name": "id",
                                "type": "Int64",
                                "unique": True,
                            },
                            {
                                "name": "name",
                                "type": "Utf8",
                            },
                            {
                                "name": "email",
                                "type": "Utf8",
                            },
                        ],
                        "sourceTransformers": [],
                        "destinationTransformers": [],
                        "insertionTransformers": [],
                    }
                ],
            }
        )

    def test_that_version_1_supports_mongodb_source(self):
        result = BeetlConfig(
            {
                "version": "V1",
                "sources": [
                    {
                        "name": "src",
                        "type": "Mongodb",
                        "connection": {
                            "settings": {
                                "database": "test",
                                "connection_string": "mongodb://localhost:27017",
                            },
                        },
                    },
                    {
                        "name": "dst",
                        "type": "Mongodb",
                        "connection": {
                            "settings": {
                                "database": "test",
                                "host": "localhost",
                                "port": "27017",
                                "username": "root",
                                "password": "root",
                            },
                        },
                    },
                ],
                "sync": [
                    {
                        "source": "src",
                        "destination": "dst",
                        "sourceConfig": {
                            "collection": "test",
                        },
                        "destinationConfig": {
                            "collection": "test",
                            "filter": {
                                "name": "John",
                            },
                            "projection": {
                                "name": 1,
                            },
                            "uniqueFields": ["id"],
                        },
                        "comparisonColumns": {"id": "Int64"},
                        "sourceTransformers": [],
                        "destinationTransformers": [],
                        "insertionTransformers": [],
                    }
                ],
            }
        )

    def test_that_version_1_supports_csv_source(self):
        result = BeetlConfig(
            {
                "version": "V1",
                "sources": [
                    {
                        "name": "src",
                        "type": "Csv",
                        "connection": {
                            "path": "test.csv",
                        },
                    },
                    {
                        "name": "dst",
                        "type": "Csv",
                        "connection": {
                            "path": "test.csv",
                        },
                    },
                ],
                "sync": [
                    {
                        "source": "src",
                        "destination": "dst",
                        "sourceConfig": {},
                        "destinationConfig": {},
                        "comparisonColumns": {"id": "Int64"},
                        "sourceTransformers": [],
                        "destinationTransformers": [],
                        "insertionTransformers": [],
                    }
                ],
            }
        )

    def test_that_version_1_supports_faker_source(self):
        result = BeetlConfig(
            {
                "version": "V1",
                "sources": [
                    {
                        "name": "src",
                        "type": "Faker",
                        "connection": {"faker": []},
                    },
                    {
                        "name": "dst",
                        "type": "Faker",
                        "connection": {"faker": []},
                    },
                ],
                "sync": [
                    {
                        "source": "src",
                        "destination": "dst",
                        "sourceConfig": {},
                        "destinationConfig": {},
                        "comparisonColumns": {"id": "Int64"},
                        "sourceTransformers": [],
                        "destinationTransformers": [],
                        "insertionTransformers": [],
                    }
                ],
            }
        )

    def test_that_version_1_supports_mysql_source(self):
        result = BeetlConfig(
            {
                "version": "V1",
                "sources": [
                    {
                        "name": "src",
                        "type": "Mysql",
                        "connection": {
                            "settings": {
                                "database": "test",
                                "host": "localhost",
                                "port": "27017",
                                "username": "root",
                                "password": "root",
                            },
                        },
                    },
                    {
                        "name": "dst",
                        "type": "Mysql",
                        "connection": {
                            "settings": {
                                "connection_string": "mysql+pymysql://root:root@localhost:3306/test",
                            },
                        },
                    },
                ],
                "sync": [
                    {
                        "source": "src",
                        "destination": "dst",
                        "sourceConfig": {
                            "table": "test",
                        },
                        "destinationConfig": {"table": "test", "uniqueColumns": ["Id"]},
                        "comparisonColumns": {"id": "Int64"},
                        "sourceTransformers": [],
                        "destinationTransformers": [],
                        "insertionTransformers": [],
                    }
                ],
            }
        )

    def test_that_version_1_supports_postgres_source(self):
        result = BeetlConfig(
            {
                "version": "V1",
                "sources": [
                    {
                        "name": "src",
                        "type": "Postgresql",
                        "connection": {
                            "settings": {
                                "connection_string": "postgresql://root:root@localhost:5432/test",
                            },
                        },
                    },
                    {
                        "name": "dst",
                        "type": "Postgresql",
                        "connection": {
                            "settings": {
                                "username": "root",
                                "password": "root",
                                "host": "localhost",
                                "port": "5432",
                                "database": "test",
                            },
                        },
                    },
                ],
                "sync": [
                    {
                        "source": "src",
                        "destination": "dst",
                        "sourceConfig": {
                            "table": "test",
                        },
                        "destinationConfig": {"table": "test", "uniqueColumns": ["Id"]},
                        "comparisonColumns": {"id": "Int64"},
                        "sourceTransformers": [],
                        "destinationTransformers": [],
                        "insertionTransformers": [],
                    }
                ],
            }
        )

    def test_that_version_1_supports_rest_source(self):
        result = BeetlConfig(
            {
                "version": "V1",
                "sources": [
                    {
                        "name": "src",
                        "type": "Rest",
                        "connection": {
                            "settings": {
                                "base_url": "https://example.com",
                                "authentication": {
                                    "basic": True,
                                    "basic_user": "root",
                                    "basic_pass": "root",
                                },
                            }
                        },
                    },
                    {
                        "name": "dst",
                        "type": "Rest",
                        "connection": {
                            "settings": {
                                "base_url": "https://example.com",
                                "authentication": {
                                    "oauth2": True,
                                    "oauth2_settings": {"flow": "implicit"},
                                },
                            }
                        },
                    },
                ],
                "sync": [
                    {
                        "source": "src",
                        "destination": "dst",
                        "sourceConfig": {
                            "listRequest": {
                                "path": "/list",
                                "method": "GET",
                                "pagination": {
                                    "enabled": True,
                                    "pageSize": 10,
                                    "startPage": 0,
                                    "pageSizeQuery": "pageSize",
                                    "pageQuery": "page",
                                    "totalQuery": "page.count",
                                    "queryIn": "query",
                                    "totalQueryIn": "body",
                                },
                            },
                        },
                        "destinationConfig": {
                            "listRequest": {},
                        },
                        "comparisonColumns": {"id": "Int64"},
                        "sourceTransformers": [],
                        "destinationTransformers": [],
                        "insertionTransformers": [],
                    }
                ],
            }
        )

    def test_that_version_1_supports_sqlserver_source(self):
        result = BeetlConfig(
            {
                "version": "V1",
                "sources": [
                    {
                        "name": "src",
                        "type": "Sqlserver",
                        "connection": {
                            "settings": {
                                "connection_string": "mssql+pyodbc://root:root@localhost:1433/test"
                            }
                        },
                    },
                    {
                        "name": "dst",
                        "type": "Sqlserver",
                        "connection": {
                            "settings": {
                                "username": "test",
                                "password": "test",
                                "host": "test",
                                "port": "test",
                                "database": "test",
                            },
                        },
                    },
                ],
                "sync": [
                    {
                        "source": "src",
                        "destination": "dst",
                        "sourceConfig": {
                            "query": "SELECT * FROM test",
                        },
                        "destinationConfig": {
                            "query": "SELECT * FROM test",
                            "table": "test",
                            "uniqueColumns": ["id"],
                        },
                        "comparisonColumns": {"id": "Int64"},
                        "sourceTransformers": [],
                        "destinationTransformers": [],
                        "insertionTransformers": [],
                    }
                ],
            }
        )

    def test_that_version_1_supports_xml_source(self):
        result = BeetlConfig(
            {
                "version": "V1",
                "sources": [
                    {
                        "name": "src",
                        "type": "Xml",
                        "connection": {
                            "path": "test.xml",
                        },
                    },
                    {
                        "name": "dst",
                        "type": "Xml",
                        "connection": {
                            "path": "test.xml",
                            "encoding": "utf-8",
                        },
                    },
                ],
                "sync": [
                    {
                        "source": "src",
                        "destination": "dst",
                        "sourceConfig": {
                            "types": {
                                "id": "Int64",
                                "name": "Utf8",
                            },
                        },
                        "destinationConfig": {
                            "unique_columns": ("id",),
                        },
                        "comparisonColumns": {"id": "Int64"},
                        "sourceTransformers": [],
                        "destinationTransformers": [],
                        "insertionTransformers": [],
                    }
                ],
            }
        )

    def test_that_version_1_supports_comparison_columns_as_list(self):
        result = BeetlConfig(
            {
                "version": "V1",
                "sources": [
                    {
                        "name": "staticsrc",
                        "type": "Static",
                        "connection": {
                            "static": [
                                {"id": 1, "name": "John", "email": "john@test.com"},
                            ],
                        },
                    },
                    {
                        "name": "staticdst",
                        "type": "Static",
                        "connection": {
                            "static": [
                                {"id": 1, "name": "John", "email": "john@test.com"},
                            ]
                        },
                    },
                ],
                "sync": [
                    {
                        "source": "staticsrc",
                        "destination": "staticdst",
                        "sourceConfig": {},
                        "destinationConfig": {},
                        "comparisonColumns": [
                            {
                                "name": "id",
                                "type": "Int64",
                                "unique": True,
                            },
                            {
                                "name": "name",
                                "type": "Utf8",
                            },
                            {
                                "name": "email",
                                "type": "Utf8",
                            },
                        ],
                        "sourceTransformers": [],
                        "destinationTransformers": [],
                        "insertionTransformers": [],
                    }
                ],
            }
        )

    def test_that_version_1_supports_comparison_columns_as_dict(self):
        result = BeetlConfig(
            {
                "version": "V1",
                "sources": [
                    {
                        "name": "staticsrc",
                        "type": "Static",
                        "connection": {
                            "static": [
                                {"id": 1, "name": "John", "email": "john@test.com"},
                            ],
                        },
                    },
                    {
                        "name": "staticdst",
                        "type": "Static",
                        "connection": {
                            "static": [
                                {"id": 1, "name": "John", "email": "john@test.com"},
                            ]
                        },
                    },
                ],
                "sync": [
                    {
                        "source": "staticsrc",
                        "destination": "staticdst",
                        "sourceConfig": {},
                        "destinationConfig": {},
                        "comparisonColumns": {"id": "Int64"},
                        "sourceTransformers": [],
                        "destinationTransformers": [],
                        "insertionTransformers": [],
                    }
                ],
            }
        )
