from typing import List
import polars as pl
from .interface import (
    register_source,
    SourceInterface,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings,
)


class CsvSourceConfiguration(SourceInterfaceConfiguration):
    """The configuration class used for static sources"""

    pass


class CsvSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """The connection configuration class used for static sources"""

    path: str
    encoding: str = "utf-8"

    def __init__(self, path: str, encoding: str = "utf-8"):
        self.path = path
        self.encoding = encoding


@register_source("csv", CsvSourceConfiguration, CsvSourceConnectionSettings)
class CsvSource(SourceInterface):
    ConnectionSettingsClass = CsvSourceConnectionSettings
    SourceConfigClass = CsvSourceConfiguration

    """ A source for static data """

    def _configure(self):
        pass

    def _connect(self):
        pass

    def _disconnect(self):
        pass

    def _query(self, params=None) -> pl.DataFrame:
        return pl.read_csv(self.connection_settings.path, encoding=self.connection_settings.encoding)

    def insert(self, data: pl.DataFrame):
        print("Inserting data into static source...")
        print(data)
        return len(data)

    def update(self, data: pl.DataFrame):
        print("Updating data in static source...")
        print(data)
        return len(data)

    def delete(self, data: pl.DataFrame):
        print("Deleting data from static source")
        print(data)
        return len(data)
