from typing import List

from polars import DataFrame as POLARS_DF

from .interface import (
    ColumnDefinition,
    SourceInterface,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings,
    register_source,
)


class FakerSourceConfiguration(SourceInterfaceConfiguration):
    """ The configuration class used for faker sources """
    columns: List[ColumnDefinition]

    def __init__(self, columns: list):
        super().__init__(columns)

class FakerSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """ The connection configuration class used for faker sources """
    data: POLARS_DF
    
    def __init__(self, faker: dict):
        self.data = POLARS_DF(faker)

@register_source('faker', FakerSourceConfiguration, FakerSourceConnectionSettings)
class FakerSource(SourceInterface):
    ConnectionSettingsClass = FakerSourceConnectionSettings
    SourceConfigClass = FakerSourceConfiguration
    
    """ A source for faker data """
    
    def _configure(self): pass
    def _connect(self): pass
    def _disconnect(self): pass
    
    def _query(self, params = None) -> POLARS_DF:
        return self.connection_settings.data
    
    def insert(self, data: POLARS_DF):
        print("Inserting data into static source...")
        print(data)
    
    def update(self, data: POLARS_DF):
        print("Updating data in static source...")
        print(data)
    
    def delete(self, data: POLARS_DF):
        print("Deleting data from static source")
        print(data)    