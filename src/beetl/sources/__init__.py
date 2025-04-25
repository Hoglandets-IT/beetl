import os
from typing import Literal

# re-export
from .csv import CsvConfig, CsvConfigArguments, CsvDiff, CsvDiffArguments, CsvSource
from .excel import (
    ExcelConfig,
    ExcelConfigArguments,
    ExcelDiff,
    ExcelDiffArguments,
    ExcelDiffConfigArguments,
    ExcelSource,
    ExcelSyncArguments,
)
from .faker import (
    FakerConfig,
    FakerConfigArguments,
    FakerDiff,
    FakerDiffArguments,
    FakerDiffConfigArguments,
    FakerSource,
)
from .interface import (
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
    MongodbDiff,
    MongodbDiffArguments,
    MongodbDiffConfigArguments,
    MongodbSource,
    MongodbSync,
    MongodbSyncArguments,
)
from .mysql import (
    MysqlConfig,
    MysqlConfigArguments,
    MysqlDiff,
    MysqlDiffArguments,
    MysqlDiffConfigArguments,
    MysqlSource,
    MysqlSyncArguments,
)
from .postgresql import (
    PostgresConfig,
    PostgresConfigArguments,
    PostgresDiff,
    PostgresDiffArguments,
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
from .static import (
    StaticConfig,
    StaticConfigArguments,
    StaticDiff,
    StaticDiffArguments,
    StaticSource,
)
from .xml import (
    XmlConfig,
    XmlConfigArguments,
    XmlDiff,
    XmlDiffArguments,
    XmlSource,
    XmlSync,
    XmlSyncArguments,
)

cDir = os.path.dirname(__file__)
for file in os.listdir(cDir):
    if file.endswith(".py") and not file.startswith("__") and file != "interface.py":
        __import__(f"{__name__}.{file[:-3]}", fromlist=["*"])
