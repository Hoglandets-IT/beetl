import polars as pl

from ..interface import SourceInterface
from ..registrated_source import register_source
from .csv_config import CsvConfig, CsvConfigArguments


@register_source("Csv")
class CsvSource(SourceInterface):
    ConfigArgumentsClass = CsvConfigArguments
    ConfigClass = CsvConfig

    """ A source for static data """

    def _configure(self):
        pass

    def _connect(self):
        pass

    def _disconnect(self):
        pass

    def _query(self, params=None) -> pl.DataFrame:
        return pl.read_csv(
            self.connection_settings.path, encoding=self.connection_settings.encoding
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
