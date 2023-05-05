
from time import perf_counter
from .data import BenchmarkData

class RestBenchmark:

    BASIC_CONFIG = {
            "version": "V1",
            "sources": [
                {
                    "name": "restsrc",
                    "type": "Rest",
                    "config": {
                        "path": "srctable",
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
                            "connection_string": "mysql://root:password@localhost:3333/database"    
                        }
                    }
                },
                {
                    "name": "mysqldst",
                    "type": "Rest",
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
                        "connection_string": "mysql://root:password@localhost:3333/database"
                        }
                    }
                }
            ],
            "sync": [
                {
                    "source": "restsrc",
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

   
    @staticmethod
    def runTestnum(amount: int, betl):
        print("Starting test with {} rows".format(amount))
        start_gen = perf_counter()
        src, dst = BenchmarkData.generateData(amount)
        
        src.to_sql(
                "srctable",
                "mysql+pymysql://root:password@localhost:3333/database",
                if_exists="replace"
        )
        
        dst.to_sql(
                "dsttable",
                "mysql+pymysql://root:password@localhost:3333/database",
                if_exists="replace"
        )
        fin = perf_counter() - start_gen
        print(f"Finished generation and insertion in {fin}s")
        
        start = perf_counter()
        amounts = betl.sync()
        tim = perf_counter() - start
        
        print(f'Finished {amounts["inserts"]} inserts,'
              f'{amounts["updates"]} updates'
              f'and {amounts["deletes"]} deletes in {tim}s')
        
        return tim
    

