import os

# Import for re-export
from .static import StaticSource, StaticSourceConnectionSettings, StaticSourceArguments
from .csv import CsvSource, CsvSourceConnectionSettings, CsvSourceArguments
from .faker import FakerSource, FakerSourceConnectionSettings, FakerSourceArguments
from .itop import ItopSource, ItopSourceConnectionSettings, ItopSourceArguments
from .mongodb import MongodbSource, MongoDBSourceConnectionSettings, MongoDBSourceConfiguration, MongoDBSourceArguments
from .interface import Sources

cDir = os.path.dirname(__file__)
for file in os.listdir(cDir):
    if file.endswith(".py") and not file.startswith("__") and file != "interface.py":
        __import__(f"{__name__}.{file[:-3]}", fromlist=["*"])
