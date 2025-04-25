"""A source for fake data"""

from polars import DataFrame

from ...diff.diff_model import Diff
from ..interface import SourceInterface
from ..registrated_source import register_source
from .faker_config import FakerConfig, FakerConfigArguments
from .faker_diff import FakerDiff, FakerDiffArguments


@register_source("Faker")
class FakerSource(SourceInterface):
    """A source for fake data"""

    ConfigArgumentsClass = FakerConfigArguments
    ConfigClass = FakerConfig
    DiffArgumentsClass = FakerDiffArguments
    DiffClass = FakerDiff

    def _configure(self):
        pass

    def _connect(self):
        pass

    def _disconnect(self):
        pass

    def _query(self, params=None) -> DataFrame:
        return self.connection_settings.data

    def insert(self, data: DataFrame):
        print("Printing insert data to console...")
        print(data)
        return len(data)

    def update(self, data: DataFrame):
        print("Printing update data to console...")
        print(data)
        return len(data)

    def delete(self, data: DataFrame):
        print("Printing delete data to console...")
        print(data)
        return len(data)

    def store_diff(self, diff: Diff):
        print("Printing diff to console...")
        print(diff.dump_json())
