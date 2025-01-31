from polars import DataFrame

from ..interface import SourceInterface
from ..registrated_source import register_source
from .faker_config import FakerConfig, FakerConfigArguments


@register_source("Faker")
class FakerSource(SourceInterface):
    ConfigArgumentsClass = FakerConfigArguments
    ConfigClass = FakerConfig

    """ A source for faker data """

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
