from dataclasses import dataclass
import polars as pl
from typing import List
import concurrent.futures

CASTABLE = (
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.Int64,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
    pl.UInt64,
    pl.Float32,
    pl.Float64,
    pl.Boolean,
    pl.Utf8,
    pl.Object,
    # Does not quite work
    # pl.Binary,
)


class RequestThreader:
    threads: int = None
    executor: concurrent.futures.ThreadPoolExecutor = None

    def __init__(self, threads: int = 10):
        print("Starting threader...")
        self.threads = threads

    def __enter__(self):
        print("Threading the needle...")
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.threads)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        print("Waiting for threads to finish...")
        self.executor.shutdown(wait=True)

    def submit(self, func, *args, **kwargs):
        print("Submitting single thread...")
        return self.executor.submit(func, *args, **kwargs)

    def submitAndWait(self, func, kwarg_list: list):
        print("Submitting a list of threads...")
        for result in self.executor.map(lambda fn: func(**fn), kwarg_list):
            yield result


def register_source(name: str, configuration: type, connection_settings: type):
    def wrapper(cls: type):
        Sources.sources[name] = {
            "class": cls,
            "configuration": configuration,
            "connection_settings": connection_settings,
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
    """The configuration class used for data sources, abstract"""

    def __init__(self):
        pass


@dataclass
class SourceInterfaceConnectionSettings:
    """The connection configuration class used for data sources, abstract"""

    pass


class SourceInterface:
    ConnectionSettingsClass = SourceInterfaceConnectionSettings
    SourceConfigClass = SourceInterfaceConfiguration

    """ Abstract interface for a connection to a data source """
    connection = None
    connection_settings = None
    source_configuration = None

    def __init__(self, connection: dict, config: dict = None) -> None:
        """Initiates a source class

        Args:
            config (dict): Coniguration for the source data (columns, IDs, etc.)
            connection (dict):
                Configuration for the source connection (paths, credentials, etc.)
        """
        try:
            self.connection_settings = self.ConnectionSettingsClass(
                **connection)
            self.source_configuration = (
                self.SourceConfigClass(**config) if config else None
            )
        except Exception as e:
            raise Exception(
                "Invalid connection settings for source: " + str(e))

        self._configure()

    def set_sourceconfig(self, config: dict) -> None:
        self.source_configuration = self.SourceConfigClass(**config)

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

    def connect(self):
        self._connect()

    def disconnect(self):
        self._disconnect()

    def query(self, params=None):
        """Formats the query according to the column specification

        Args:
            df (DataFrame): The data from the data source
        """
        return self._query(params)

    def _query(self, params=None) -> pl.DataFrame:
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
