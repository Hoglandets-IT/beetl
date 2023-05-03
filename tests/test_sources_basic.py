import unittest
from src.beetl import beetl

class TestStaticSource(unittest.TestCase):
    """Basic functionality test for functions in src/beetl/beetl.py"""
    def setUp(self):
        self.basicConfig = {
                "version": "V1",
                "sources": [
                    {
                        "name": "staticsrc",
                        "type": "Static",
                        "config": {
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
                            "static": [
                                {
                                    "id": 1,
                                    "name": "John",
                                    "email": "john@test.com"
                                },
                                {
                                    "id": 2,
                                    "name": "Jane",
                                    "email": "jane@test.com"
                                },
                                {
                                    "id": 3,
                                    "name": "Steffen",
                                    "email": "steffen@test.com"
                                }
                            ]
                        }
                    },
                    {
                        "name": "staticdst",
                        "type": "Static",
                        "config": {
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
                            "static": [
                                {
                                    "id": 1,
                                    "name": "John",
                                    "email": "john@test.com"
                                },
                                {
                                    "id": 4,
                                    "name": "James",
                                    "email": "jane@test.com"
                                },
                                {
                                    "id": 3,
                                    "name": "Stephen",
                                    "email": "stephen@test.com"
                                }
                            ]
                        }
                    }
                ],
                "sync": [
                    {
                        "source": "staticsrc",
                        "destination": "staticdst",
                        "fieldTransformers": [
                            {
                                "transformer": "strings.lowercase",
                                "config": {
                                    "inField": "name",
                                    "outField": "nameL"
                                }
                            },
                            {
                                "transformer": "strings.uppercase",
                                "config": {
                                    "inField": "name",
                                    "outField": "nameU"
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
                                        "nameL",
                                        "nameU"
                                    ],
                                    "outField": "name",
                                    "separator": " ^-^ "
                                }
                            },
                            {
                                "transformer": "frames.drop_columns",
                                "config": {
                                    "columns": [
                                        "nameL",
                                        "nameU"
                                    ]
                                }
                            }
                        ]
                    }
                ]
            }
    
    def test_static(self):
        betl = beetl.Beetl(beetl.BeetlConfig(self.basicConfig))
        amounts = betl.sync()
        
        self.assertEqual(
            amounts,
            {
                "inserts": 1,
                "updates": 2,
                "deletes": 1
            }
        )
        
class TestMysqlSource(unittest.TestCase):
    """Basic functionality test for functions in src/beetl/beetl.py"""
    def setUp(self):
        self.basicConfig = {
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
                            "connection_string": "mysql://root:password@10.167.100.222:3333/database"    
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
                               "connection_string": "mysql://root:password@10.167.100.222:3333/database"
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
        betl = beetl.Beetl(beetl.BeetlConfig(self.basicConfig))
        amounts = betl.sync()
        
        print(amounts)
    
    def test_mysql(self):
        betl = beetl.Beetl(beetl.BeetlConfig(self.basicConfig))
        amounts = betl.sync()
        
        self.assertEqual(
            amounts,
            {
                "inserts": 1,
                "updates": 1,
                "deletes": 1
            }
        )
        
        # When running again, the result should be 0, 0, 0
        amountsTwo = beetl.sync()
        self.assertEqual(
            amountsTwo,
            {
                "inserts": 0,
                "updates": 0,
                "deletes": 0
            }
        )
        