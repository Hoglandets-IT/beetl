from typing import Annotated
import polars as pl
from pydantic import Field
from .interface import (
    register_source,
    SourceInterface,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettingsArguments,
    SourceInterfaceConnectionSettings
)


class CsvSourceConnectionSettingsArguments(SourceInterfaceConnectionSettingsArguments):
    path: Annotated[str, Field(min_length=1)]
    encoding: Annotated[str, Field(default="utf-8")]


class CsvSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """The connection configuration class used for static sources"""

    path: str
    encoding: str

    def __init__(self, arguments: CsvSourceConnectionSettingsArguments):
        super().__init__(arguments)
        self.path = arguments.path
        self.encoding = arguments.encoding


@register_source("csv", SourceInterfaceConfiguration, CsvSourceConnectionSettings)
class CsvSource(SourceInterface):
    ConnectionSettingsArguments = CsvSourceConnectionSettingsArguments
    ConnectionSettingsClass = CsvSourceConnectionSettings
    SourceConfigClass = SourceInterfaceConfiguration

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
