import os

# Import for re-export
from .static import StaticSource, StaticConfig, StaticConfigArguments
from .csv import CsvSource, CsvConfig, CsvConfigArguments
from .faker import FakerSource, FakerConfig, FakerConfigArguments
from .itop import ItopSource, ItopConfig, ItopConfigArguments
from .mongodb import MongodbSource, MongodbConfig, MongodbSync, MongodbConfigArguments
from .mysql import MysqlSource, MysqlConfig, MysqlConfigArguments
from .postgresql import PostgresSource, PostgresConfig, PostgresConfigArguments
from .interface import Sources

cDir = os.path.dirname(__file__)
for file in os.listdir(cDir):
    if file.endswith(".py") and not file.startswith("__") and file != "interface.py":
        __import__(f"{__name__}.{file[:-3]}", fromlist=["*"])
