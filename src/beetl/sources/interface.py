from abc import ABC, abstractmethod
from dataclasses import dataclass
import polars as pol
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
    """
    name: str
    type: pol.DataType
    unique: bool = False
    skip_update: bool = False
    
    def __post_init__(self) -> None:
        """Get the actual data type from Polars"""
        self.type = getattr(pol, self.type)

@dataclass
class SourceInterfaceConfiguration:
    """ The configuration class used for data sources, abstract"""
    columns: List[ColumnDefinition]
    
    def __init__(self, columns: list):
        self.columns = [ColumnDefinition(**col) for col in columns]

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
    
    def query(self, params = None) -> pol.DataFrame:
        """Run a query on the source and return the results

        Args:
            params (mixed, optional): Parameters for the query. Defaults to None.

        Returns:
            pol.DataFrame: The results of the query
        """
        raise NotImplementedError
    
    def insert(self, data: pol.DataFrame) -> None:
        """Insert data into the source

        Args:
            data (pol.DataFrame): The data to insert
        """
        raise NotImplementedError

    def update(self, data: pol.DataFrame) -> None:
        """Update data in the source

        Args:
            data (pol.DataFrame): The data to update
        """
        raise NotImplementedError
    
    def delete(self, data: pol.DataFrame) -> None:
        """Delete data from the source

        Args:
            data (pol.DataFrame): The data to delete
        """
        raise NotImplementedError