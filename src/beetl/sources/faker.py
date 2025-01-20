from polars import DataFrame as POLARS_DF
from pydantic import ConfigDict
from .interface import (
    SourceInterface,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings,
    register_source,
)


class FakerSourceConfiguration(SourceInterfaceConfiguration):
    pass


class FakerSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """The connection configuration class used for faker sources"""

    # Arbitrary types are allow since dataframe is not a pydantic type
    model_config = ConfigDict(arbitrary_types_allowed=True)

    data: POLARS_DF

    def __init__(self, **kwargs):
        self.data = POLARS_DF(kwargs.get("faker", []))
        super().__init__(**kwargs)


@register_source("faker", FakerSourceConfiguration, FakerSourceConnectionSettings)
class FakerSource(SourceInterface):
    ConnectionSettingsClass = FakerSourceConnectionSettings
    SourceConfigClass = FakerSourceConfiguration

    """ A source for faker data """

    def _configure(self):
        raise Exception("Not yet implemented")

    def _connect(self):
        pass

    def _disconnect(self):
        pass

    def _query(self, params=None) -> POLARS_DF:
        return self.connection_settings.data

    def insert(self, data: POLARS_DF):
        print("Inserting data into static source...")
        print(data)
        return len(data)

    def update(self, data: POLARS_DF):
        print("Updating data in static source...")
        print(data)
        return len(data)

    def delete(self, data: POLARS_DF):
        print("Deleting data from static source")
        print(data)
        return len(data)
