import polars as pl
import sqlalchemy as sqla
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
        
        self.connection_string = "mysql+pymysql://"
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
    
    def _query(self, params = None, customQuery: str = None, returnData: bool = True) -> pl.DataFrame:
        query = customQuery
        
        if query is None:
            
            if self.source_configuration.query is not None:
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

        if returnData:
            try:
                df = pl.read_sql(
                    query=query,
                    connection_uri=self.connection_settings.connection_string
                )
            except ModuleNotFoundError:
                df = pl.read_sql(
                    query=query,
                    connection_uri=self.connection_settings.connection_string.replace(
                        'mysql://', 'mysql+pymysql://'
                    )
                )
            
            return df
        try:
            with sqla.create_engine(self.connection_settings.connection_string).connect() as con:
                res = con.execute(query)
                con.commit()
        except ModuleNotFoundError:
            with sqla.create_engine(
                self.connection_settings.connection_string.replace(
                    'mysql://', 'mysql+pymysql://'
            )).connect() as con:
                
                res = con.execute(sqla.text(query))
                con.commit()

    
    def _insert(self, data: pl.DataFrame, table: str = None, connection_string: str = None):
        if table is None:
            table = self.source_configuration.table
        if connection_string is None:
            connection_string = self.connection_settings.connection_string
        
        try:
            data.write_database(
                table,
                self.connection_settings.connection_string,
                if_exists="append"
            )
        except ModuleNotFoundError:
            data.write_database(
                table,
                self.connection_settings.connection_string.replace(
                    'mysql://', 'mysql+pymysql://'
                ),
                if_exists="append"
            )
    
        return len(data)
    
    def insert(self, data: pl.DataFrame):
        return self._insert(data)
    
    def update(self, data: pl.DataFrame):
        tempDB = self.source_configuration.table + "_udTemp"
        query = f"CREATE TEMPORARY TABLE {tempDB} LIKE {self.source_configuration.table}"
        
        # Create temporary table       
        self._query(customQuery=query, returnData=False)
        
        # Insert data into temporary table
        self._insert(data, table=tempDB)
        
        # Run the update query
        on_clause = " AND \n".join(
            f"dst.{fld.name} = src.{fld.name}" for fld in self.source_configuration.columns if fld.unique
        )
        
        update_clause = ", \n".join(
            f"dst.{fld.name} = src.{fld.name}" for fld in self.source_configuration.columns if not (fld.unique or fld.skip_update)
        )
        
        query = f"""
            UPDATE
                {self.source_configuration.table} as dst
            INNER JOIN
                {tempDB} as src
            ON
                {on_clause}
            SET
                {update_clause}
        """
        
        self._query(customQuery=query, returnData=False)
        
        return len(data)
    
    def delete(self, data: pl.DataFrame): 
        return len(data)