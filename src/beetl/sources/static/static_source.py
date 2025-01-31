from polars import DataFrame

from ..interface import SourceInterface
from ..registrated_source import register_source
from .static_config import StaticConfig, StaticConfigArguments


@register_source("Static")
class StaticSource(SourceInterface):
    ConfigArgumentsClass = StaticConfigArguments
    ConfigClass = StaticConfig

    """ A source for static data """

    def _configure(self):
        pass

    def _connect(self):
        pass

    def _disconnect(self):
        pass

    def _query(self, params=None) -> DataFrame:
        return self.connection_settings.data

    def insert(self, data: DataFrame):
        print("Inserting data into static source...")
        print(data)
        return len(data)

    def update(self, data: DataFrame):
        print("Updating data in static source...")
        print(data)
        return len(data)

    def delete(self, data: DataFrame):
        print("Deleting data from static source")
        print(data)
        return len(data)
