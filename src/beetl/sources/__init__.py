import os

# re-export
from .csv import CsvConfig, CsvConfigArguments, CsvSource
from .faker import FakerConfig, FakerConfigArguments, FakerSource
from .interface import Sources
from .itop import ItopConfig, ItopConfigArguments, ItopSource
from .mongodb import MongodbConfig, MongodbConfigArguments, MongodbSource, MongodbSync
from .mysql import MysqlConfig, MysqlConfigArguments, MysqlSource
from .postgresql import PostgresConfig, PostgresConfigArguments, PostgresSource
from .rest import RestConfig, RestConfigArguments, RestSource
from .sqlserver import (
    SqlserverConfig,
    SqlserverConfigArguments,
    SqlserverSource,
    SqlserverSync,
)
from .static import StaticConfig, StaticConfigArguments, StaticSource

cDir = os.path.dirname(__file__)
for file in os.listdir(cDir):
    if file.endswith(".py") and not file.startswith("__") and file != "interface.py":
        __import__(f"{__name__}.{file[:-3]}", fromlist=["*"])
