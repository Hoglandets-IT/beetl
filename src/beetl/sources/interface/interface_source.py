from typing import Literal

import polars as pl

from ...diff import Diff
from .interface_config import SourceConfig, SourceConfigArguments
from .interface_diff import SourceDiff, SourceDiffArguments
from .interface_sync import SourceSync, SourceSyncArguments


class SourceInterface:
    ConfigClass = SourceConfig
    ConfigArgumentsClass = SourceConfigArguments
    SyncClass = SourceSync
    SyncArgumentsClass = SourceSyncArguments
    DiffClass = SourceDiff
    DiffArgumentsClass = SourceDiffArguments

    """ Abstract interface for a connection to a data source """
    connection = None
    connection_settings_arguments = None
    connection_settings = None
    source_configuration_arguments = None
    source_configuration = None
    diff_config_arguments: SourceDiffArguments = None
    diff_config: SourceDiff = None

    def __init__(self, source: dict) -> None:
        """Initiates a source class

        Args:
            config (dict): Coniguration for the source data (columns, IDs, etc.)
            connection (dict):
                Configuration for the source connection (paths, credentials, etc.)
        """

        self.connection_settings_arguments = self.ConfigArgumentsClass(**source)

        self.connection_settings = self.ConfigClass(self.connection_settings_arguments)

        self._configure()

    def set_sourceconfig(
        self,
        config: dict,
        direction: Literal["source", "destination"],
        name: str,
        location: tuple[str],
    ) -> None:
        self.source_configuration_arguments = self.SyncArgumentsClass(
            direction=direction, name=name, location=location, **config
        )
        self.source_configuration = self.SyncClass(self.source_configuration_arguments)

    def set_diff_config(self, diff_config: dict) -> None:
        self.diff_config_arguments = self.DiffArgumentsClass(**diff_config)
        self.diff_config = self.DiffClass(self.diff_config_arguments)

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

    def store_diff(self, diff: Diff):
        """Store the diff in the source

        Args:
            diff (Diff): The diff to store
        """
        raise NotImplementedError
