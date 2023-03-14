from abc import ABC, abstractmethod
from dataclasses import dataclass
import polars as pol
from typing import List

@dataclass
class DataColumn:
    name: str
    type: pol.DataType
    unique: bool = False
    skip_update: bool = False
    
    def __post_init__(self):
        self.type = getattr(pol, self.type)

@dataclass
class SourceInterfaceConfig:
    columns: List[DataColumn]
    
    def __post_init__(self):
        self._format_columns()
    
    def _format_columns(self):
        self.columns = [DataColumn(**col) for col in self.columns]

@dataclass
class SourceConnectionSettings:
    pass

class SourceInterface(ABC):
    ConnectionSettingsClass = SourceConnectionSettings
    SourceConfigClass = SourceInterfaceConfig
    
    """Interface for a connection to a data source, either for reading or writing data"""
    connection = None
    connection_settings = None
    source_config = None
    
    def __init__(self, config: dict, connection: dict) -> None:
        """Initiates a Source object

        Args:
            source_config (dict): Configuration for the source data (columns, unique IDs, etc.)
            connection_settings (dict): Configuration for the source (connection information, username, password, etc.)
        """
        try:
            self.connection_settings = self.ConnectionSettingsClass(**connection)
            self.source_config = self.SourceConfigClass(**config)
        except Exception:
            raise Exception("Invalid connection settings for source")  
        
        self._configure()
    
    def __enter__(self):
        self._connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._disconnect()
    
    def _configure(self):
        """ Commands required to configure the connection to the source

        Raises:
            NotImplementedError: Needs to be implemented in child class
        """
        raise NotImplementedError 
    
    def _connect(self):
        """ Code for starting the connection to the source (optional)
        
        """
        pass
    
    def _disconnect(self):
        """ Code for ending the connection to the source (optional)
        """
        pass
    
    @abstractmethod
    def query(self, *args, **kwargs) -> pol.DataFrame:
        """Query the data source and retrieve a Polars Dataframe object

        Raises:
            NotImplementedError: Needs to be implemented in child class

        Returns:
            pol.DataFrame: Dataframe with the results of the query
        """
        raise NotImplementedError

    @abstractmethod
    def insert(self, data: pol.DataFrame):
        """Insert the data in the dataframe into the source

        Args:
            data (pol.DataFrame): The data to be inserted

        Raises:
            NotImplementedError: Needs to be implemented in the child class
        """
        raise NotImplementedError
    
    @abstractmethod
    def update(self, data: pol.DataFrame):
        """Update the data in the dataframe into the source

        Args:
            data (pol.DataFrame): The data to be updated

        Raises:
            NotImplementedError: Needs to be implemented in the child class
        """
        raise NotImplementedError
    
    @abstractmethod
    def delete(self, data: pol.DataFrame):
        """Delete the given rows from the data source

        Args:
            data (pol.DataFrame): The rows to be deleted

        Raises:
            NotImplementedError: Needs to be implemented in the child class
        """
        raise NotImplementedError
