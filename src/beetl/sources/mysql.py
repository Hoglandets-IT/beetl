from typing import List
from polars import DataFrame as POLARS_DF
from .interface import (
    register_source,
    SourceInterface,
    ColumnDefinition,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings
)

class MysqlSourceConfiguration(SourceInterfaceConfiguration):
    """ The configuration class used for MySQL sources """
    columns: List[ColumnDefinition]

    def __init__(self, columns: list):
        super().__init__(columns)

class MysqlSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """ The connection configuration class used for MySQL sources """
    data: POLARS_DF
    
    def __init__(self, settings: dict):
        pass

@register_source('mysql', MysqlSourceConfiguration, MysqlSourceConnectionSettings)
class MysqlSource(SourceInterface):
    ConnectionSettingsClass = MysqlSourceConnectionSettings
    SourceConfigClass = MysqlSourceConfiguration
    
    """ A source for MySQL data """
    
    def _configure(self): pass
    def _connect(self): pass
    def _disconnect(self): pass
    
    def query(self, params = None) -> POLARS_DF: pass
    
    def insert(self, data: POLARS_DF): pass
    
    def update(self, data: POLARS_DF): pass
    
    def delete(self, data: POLARS_DF): pass