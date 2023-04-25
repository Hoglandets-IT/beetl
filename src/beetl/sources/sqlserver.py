from typing import List
from polars import DataFrame as POLARS_DF
from .interface import (
    register_source,
    SourceInterface,
    ColumnDefinition,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings
)

class SQLServerConfiguration(SourceInterfaceConfiguration):
    """ The configuration class used for SQL Server sources """
    columns: List[ColumnDefinition]

    def __init__(self, columns: list):
        super().__init__(columns)

class SQLServerConnectionSettings(SourceInterfaceConnectionSettings):
    """ The connection configuration class used for SQL Server sources """
    data: POLARS_DF
    
    def __init__(self, settings: dict):
        pass

@register_source('sqlserver', SQLServerConfiguration, SQLServerConnectionSettings)
class SQLServer(SourceInterface):
    ConnectionSettingsClass = SQLServerConnectionSettings
    SourceConfigClass = SQLServerConfiguration
    
    """ A source for SQL Server data """
    
    def _configure(self): pass
    def _connect(self): pass
    def _disconnect(self): pass
    
    def _query(self, params = None) -> POLARS_DF: pass
    
    def insert(self, data: POLARS_DF): pass
    
    def update(self, data: POLARS_DF): pass
    
    def delete(self, data: POLARS_DF): pass