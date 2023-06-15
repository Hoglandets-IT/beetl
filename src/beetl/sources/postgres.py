from typing import List
from polars import DataFrame as POLARS_DF
from .interface import (
    register_source,
    SourceInterface,
    ColumnDefinition,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings,
)


class PostgresSourceConfiguration(SourceInterfaceConfiguration):
    """The configuration class used for Postgresql sources"""

    columns: List[ColumnDefinition]

    def __init__(self, columns: list):
        super().__init__(columns)


class PostgresSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """The connection configuration class used for Postgresql sources"""

    data: POLARS_DF

    def __init__(self, settings: dict):
        pass


@register_source(
    "postgres", PostgresSourceConfiguration, PostgresSourceConnectionSettings
)
class PostgresSource(SourceInterface):
    ConnectionSettingsClass = PostgresSourceConnectionSettings
    SourceConfigClass = PostgresSourceConfiguration

    """ A source for Postgresql data """

    def _configure(self):
        raise Exception("Not yet implemented")

    def _connect(self):
        pass

    def _disconnect(self):
        pass

    def _query(self, params=None) -> POLARS_DF:
        pass

    def insert(self, data: POLARS_DF):
        pass

    def update(self, data: POLARS_DF):
        pass

    def delete(self, data: POLARS_DF):
        pass
