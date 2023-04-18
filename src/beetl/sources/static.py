from typing import List
from polars import DataFrame as POLARS_DF
from .interface import (
    register_source,
    SourceInterface,
    ColumnDefinition,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings
)

class StaticSourceConfiguration(SourceInterfaceConfiguration):
    """ The configuration class used for static sources """
    columns: List[ColumnDefinition]

    def __init__(self, columns: list):
        super().__init__(columns)

class StaticSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """ The connection configuration class used for static sources """
    data: POLARS_DF
    
    def __init__(self, static: dict):
        self.data = POLARS_DF(static)

@register_source('static', StaticSourceConfiguration, StaticSourceConnectionSettings)
class StaticSource(SourceInterface):
    ConnectionSettingsClass = StaticSourceConnectionSettings
    SourceConfigClass = StaticSourceConfiguration
    
    """ A source for static data """
    
    def _configure(self): pass
    def _connect(self): pass
    def _disconnect(self): pass
    
    def query(self, params = None) -> POLARS_DF:
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