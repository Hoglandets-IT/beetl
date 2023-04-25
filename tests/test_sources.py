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
                            "table": "testtable",
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
                            "connection_string": "mysql://root@localhost:3306/srcdb"    
                            }
                            
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
                        "source": "mysqlsrc",
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
    
    def test_mysql(self):
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
        