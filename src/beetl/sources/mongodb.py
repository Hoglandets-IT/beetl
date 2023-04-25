from typing import List
from polars import DataFrame as POLARS_DF
from .interface import (
    register_source,
    SourceInterface,
    ColumnDefinition,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings
)

class MongoDBSourceConfiguration(SourceInterfaceConfiguration):
    """ The configuration class used for MongoDB sources """
    columns: List[ColumnDefinition]

    def __init__(self, columns: list):
        super().__init__(columns)

class MongoDBSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """ The connection configuration class used for MongoDB sources """
    data: POLARS_DF
    
    def __init__(self, settings: dict):
        pass

@register_source('mongodb', MongoDBSourceConfiguration, MongoDBSourceConnectionSettings)
class MongoDBSource(SourceInterface):
    ConnectionSettingsClass = MongoDBSourceConnectionSettings
    SourceConfigClass = MongoDBSourceConfiguration
    
    """ A source for MongoDB data """
    
    def _configure(self): pass
    def _connect(self): pass
    def _disconnect(self): pass
    
    def _query(self, params = None) -> POLARS_DF: pass
    
    def insert(self, data: POLARS_DF): pass
    
    def update(self, data: POLARS_DF): pass
    
    def delete(self, data: POLARS_DF): pass