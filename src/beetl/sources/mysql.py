import polars as pl
from typing import List
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
    table: str = None
    query: str = None

    def __init__(self, columns: list, table: str = None, query: str = None):
        super().__init__(columns)
        self.table = table
        self.query = query
        

class MysqlSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """ The connection configuration class used for MySQL sources """
    data: pl.DataFrame
    connection_string: str
    query: str = None
    table: str = None
    
    def __init__(self, settings: dict):
        if settings.get('connection_string', False):
            self.connection_string = settings['connection_string']
            return
        
        self.connection_string = "mysql://"
        f"{settings['username']}:{settings['password']}"
        f"@{settings['host']}:{settings['port']}/{settings['database']}"
        

@register_source('mysql', MysqlSourceConfiguration, MysqlSourceConnectionSettings)
class MysqlSource(SourceInterface):
    ConnectionSettingsClass = MysqlSourceConnectionSettings
    SourceConfigClass = MysqlSourceConfiguration
    
    """ A source for MySQL data """
    
    def _configure(self): pass
    def _connect(self): pass
    def _disconnect(self): pass
    
    def _query(self, params = None) -> pl.DataFrame:
        query = self.source_configuration.query
        
        if query is None:
            if self.source_configuration.table is None:
                raise Exception("No query or table specified")
            
            cols = ",".join(
                [
                    "`" + col.name + "`" for col in self.source_configuration.columns
                ]
            )
            
            query = f"SELECT {cols} FROM {self.source_configuration.table}"
            
        df = pl.read_sql(
            query=query,
            connection_uri=self.connection_settings.connection_string
        )
        
        return df
    
    def insert(self, data: pl.DataFrame):
        data.to_pandas().to_sql(
            self.source_configuration.table,
            self.connection_settings.connection_string
        )
        return len(data)
    
    def update(self, data: pl.DataFrame): 
        return len(data)
    
    def delete(self, data: pl.DataFrame): 
        return len(data)