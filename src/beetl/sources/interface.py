from abc import ABC, abstractmethod
from dataclasses import dataclass
import polars as pl
from typing import List

def register_source(name: str, configuration: type, connection_settings: type):
    def wrapper(cls: type):
        Sources.sources[name] = {
            "class": cls,
            "configuration": configuration,
            "connection_settings": connection_settings
        }
        return cls
    return wrapper

class Sources:
    sources: dict = {}

@dataclass
class ColumnDefinition:
    """The definition of a column in a dataset.
    Name: Name of column
    Type: Polars data type
    Unique: Whether it is unique
    Skip Update: Whether to skip updating this field when inserting/updating source
    Custom Options: Custom options for some providers
    """
    name: str
    type: pl.DataType
    unique: bool = False
    skip_update: bool = False
    custom_options: dict = None
    
    def __post_init__(self) -> None:
        """Get the actual data type from Polars"""
        self.type = getattr(pl, self.type)

@dataclass
class SourceInterfaceConfiguration:
    """ The configuration class used for data sources, abstract"""
    columns: List[ColumnDefinition]
    unique_columns: List[str] = None
    comparison_columns: List[str] = None
    skip_columns: List[str] = None
    
    def __init__(self, columns: list):
        self.columns = [ColumnDefinition(**col) for col in columns]
        self.unique_columns = [col.name for col in self.columns if col.unique]
        self.comparison_columns = [col.name for col in self.columns if (not col.unique and not col.skip_update)]
        self.skip_columns = [col.name for col in self.columns if col.skip_update]

@dataclass
class SourceInterfaceConnectionSettings:
    """ The connection configuration class used for data sources, abstract"""
    pass

class SourceInterface:
    ConnectionSettingsClass = SourceInterfaceConnectionSettings
    SourceConfigClass = SourceInterfaceConfiguration
    
    """ Abstract interface for a connection to a data source """
    connection = None
    connection_settings = None
    source_configuration = None
    
    def __init__(self, config: dict, connection: dict) -> None:
        """Initiates a source class

        Args:
            config (dict): Coniguration for the source data (columns, IDs, etc.)
            connection (dict): Configuration for the source connection (paths, credentials, etc.)
        """
        try:
            self.connection_settings = self.ConnectionSettingsClass(**connection)
            self.source_configuration = self.SourceConfigClass(**config)
        except Exception as e:
            raise Exception("Invalid connection settings for source: " + str(e))
        
        self._configure()
    
    def __enter__(self):
        self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._disconnect()
    
    def _configure(self):
        """
            Method responsible for configuring the connection to 
            the dataset. Should be overridden in child class. 
            Called during init.
        """
        raise NotImplementedError
    
    def _connect(self):
        """
            Method responsible for connecting to the dataset.
            Should be overridden in child class. Called during init.
        """
        raise NotImplementedError

    def _disconnect(self):
        """
            Method responsible for disconnecting from the dataset.
            Should be overridden in child class. Called during exit.
        """
        raise NotImplementedError
    
    def query(self, params = None):
        """Formats the query according to the column specification

        Args:
            df (DataFrame): The data from the data source
        """
        df = self._query(params)
        
        for col in self.source_configuration.columns:
            if col.name in df.columns:
                df = df.with_columns(pl.col(col.name).cast(col.type))
        
        return df
    
    def _query(self, params = None) -> pl.DataFrame:
        """Run a query on the source and return the results

        Args:
            params (mixed, optional): Parameters for the query. Defaults to None.

        Returns:
            pl.DataFrame: The results of the query
        """
        raise NotImplementedError
    
    def insert(self, data: pl.DataFrame) -> None:
        """Insert data into the source

        Args:
            data (pl.DataFrame): The data to insert
        """
        raise NotImplementedError

    def update(self, data: pl.DataFrame) -> None:
        """Update data in the source

        Args:
            data (pl.DataFrame): The data to update
        """
        raise NotImplementedError
    
    def delete(self, data: pl.DataFrame) -> None:
        """Delete data from the source

        Args:
            data (pl.DataFrame): The data to delete
        """
        raise NotImplementedError