
from time import perf_counter

from .data import BenchmarkData


class MsSQLBenchmark:

    BASIC_CONFIG = {
        "version": "V1",
        "sources": [
            {
                "name": "sqlserversrc",
                "type": "Sqlserver",
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
                        "connection_string": "mssql://sa:Password123@localhost:3334/database?TrustServerCertificate=Yes"
                    }

                }
            },
            {
                "name": "mysqldst",
                "type": "Sqlserver",
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
                        "connection_string": "mssql://sa:Password123@localhost:3334/database?TrustServerCertificate=Yes"
                    }
                }
            }
        ],
        "sync": [
            {
                "source": "sqlserversrc",
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
            "mssql://sa:Password123@localhost:3334/database?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=Yes",
            if_exists="replace"
        )

        dst.to_sql(
            "dsttable",
            "mssql://sa:Password123@localhost:3334/database?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=Yes",
            if_exists="replace"
        )
        fin = perf_counter() - start_gen
        print(f"Finished generation and insertion in {fin}s")

        start = perf_counter()
        result = betl.sync()
        tim = perf_counter() - start

        print(f'Finished {result.inserts} inserts,'
              f'{result.updates} updates'
              f'and {result.deletes} deletes in {tim}s')

        return tim
