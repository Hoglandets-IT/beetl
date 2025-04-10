import polars as pl

from .excel_sync import ExcelSync, ExcelSyncArguments

from ..interface import SourceInterface
from ..registrated_source import register_source
from .excel_config import ExcelConfig, ExcelConfigArguments


@register_source("Excel")
class ExcelSource(SourceInterface):
    ConfigArgumentsClass = ExcelConfigArguments
    ConfigClass = ExcelConfig
    SyncClass = ExcelSync
    SyncArgumentsClass = ExcelSyncArguments

    connection_settings_arguments: ExcelConfigArguments = None
    connection_settings: ExcelConfig = None
    source_configuration_arguments: ExcelSyncArguments = None
    source_configuration: ExcelSync = None

    """ A source for static data """

    def _configure(self):
        pass

    def _connect(self):
        pass

    def _disconnect(self):
        pass

    def _query(self, params=None) -> pl.DataFrame:
        return pl.read_excel(
            self.connection_settings.path,
            schema_overrides=self.source_configuration.types,
        )

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
