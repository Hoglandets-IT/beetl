from polars import DataFrame as POLARS_DF
from .interface import (
    register_source,
    SourceInterface,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings,
)


class MongoDBSourceConfiguration(SourceInterfaceConfiguration):
    """The configuration class used for MongoDB sources"""

    pass


class MongoDBSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """The connection configuration class used for MongoDB sources"""

    pass


@register_source("mongodb", MongoDBSourceConfiguration, MongoDBSourceConnectionSettings)
class MongoDBSource(SourceInterface):
    ConnectionSettingsClass = MongoDBSourceConnectionSettings
    SourceConfigClass = MongoDBSourceConfiguration

    """ A source for MongoDB data """

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
