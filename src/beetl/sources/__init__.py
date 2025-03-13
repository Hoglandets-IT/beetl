import os
from typing import Literal

# re-export
from .csv import CsvConfig, CsvConfigArguments, CsvSource
from .excel import ExcelConfig, ExcelConfigArguments, ExcelSource
from .faker import FakerConfig, FakerConfigArguments, FakerSource
from .interface import (
    CASTABLE,
    SourceConfig,
    SourceConfigArguments,
    SourceConnectionArguments,
    SourceDiff,
    SourceDiffArguments,
    SourceDiffConfigArguments,
    SourceSync,
    SourceSyncArguments,
)
from .itop import ItopConfig, ItopConfigArguments, ItopSource, ItopSyncArguments
from .mongodb import (
    MongodbConfig,
    MongodbConfigArguments,
    MongodbSource,
    MongodbSync,
    MongodbSyncArguments,
)
from .mysql import MysqlConfig, MysqlConfigArguments, MysqlSource, MysqlSyncArguments
from .postgresql import (
    PostgresConfig,
    PostgresConfigArguments,
    PostgresSource,
    PostgresSyncArguments,
)
from .registrated_source import Sources, register_source
from .request_threader import RequestThreader
from .rest import RestConfig, RestConfigArguments, RestSource, RestSyncArguments
from .sqlserver import (
    SqlserverConfig,
    SqlserverConfigArguments,
    SqlserverDiff,
    SqlserverDiffArguments,
    SqlserverDiffConfigArguments,
    SqlserverSource,
    SqlserverSync,
    SqlserverSyncArguments,
)
from .static import StaticConfig, StaticConfigArguments, StaticSource
from .xml import XmlConfig, XmlConfigArguments, XmlSource, XmlSync, XmlSyncArguments

SourceTypes = Literal[
    "Sqlserver",
    "Mysql",
    "Postgresql",
    "Mongodb",
    "Itop",
    "Rest",
    "Csv",
    "Faker",
    "Static",
    "Xml",
    "Excel",
]

cDir = os.path.dirname(__file__)
for file in os.listdir(cDir):
    if file.endswith(".py") and not file.startswith("__") and file != "interface.py":
        __import__(f"{__name__}.{file[:-3]}", fromlist=["*"])
