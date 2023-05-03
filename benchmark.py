from faker import Faker
from src.beetl import beetl
from random import randint, shuffle
import pandas as pd
from time import perf_counter


BASIC_CONFIG = {
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

faker = Faker()
def generateData(amount: int):
    fakeSrc = []
    fakeDst = []
    
    for i in range(amount):
        obj = {
            "id": i,
            "name": faker.name(),
            "email": faker.email()
        }
        
        # Lower than 33: insert (remove from dst)
        if i < amount//3:
            fakeSrc.append(dict(obj))
        elif i > (amount//3)*2:
            fakeDst.append(dict(obj))
        else:
            fakeSrc.append(dict(obj))
            fakeDst.append(dict(obj)) 
    
    # Updates
    for i in range(0, amount//3):
        if randint(0, 1) == 1:
            fakeDst[i]["name"] = fakeDst[i]["name"][::-1]
        if randint(0, 1) == 1:
            fakeDst[i]["email"] = fakeDst[i]["email"][::-1]
    
    return pd.DataFrame(fakeSrc), pd.DataFrame(fakeDst)

def runTestnum(amount: int, betl):
    print("Starting test with {} rows".format(amount))
    start_gen = perf_counter()
    src, dst = generateData(amount)
    
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
    
    print(f'Finished {amounts["inserts"]} inserts, {amounts["updates"]} updates and {amounts["deletes"]} deletes in {tim}s')   
    

if __name__ == '__main__':
    amounts = [
        # 100, 1-2s
        # 1000, 2-3s
        # 10000, ~20-40s
        20000,
        40000,
        60000,
        100000
    ]
    
    betl = beetl.Beetl(beetl.BeetlConfig(BASIC_CONFIG))

    for amount in amounts:
        runTestnum(amount, betl)
        print()