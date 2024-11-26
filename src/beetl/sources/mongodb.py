from typing import List
from polars import DataFrame
from .interface import (
    ColumnDefinition,
    register_source,
    SourceInterface,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings,
)
from pymongoarrow.api import find_polars_all
from pymongo import MongoClient
import polars


class MongoDBSourceConfiguration(SourceInterfaceConfiguration):
    """The configuration class used for MongoDB sources"""
    columns: List[ColumnDefinition]
    unique_columns: List[str] = None
    skip_columns: List[str] = None
    collection: str = None
    filter: str = None
    projection: dict = None
    comparison_columns: List[str] = None
    unique_columns: List[str] = None

    def __init__(self, columns: list, collection: str = None, filter: dict = {}, projection: dict = {}, comparison_columns: list = None,
                 unique_columns: list = None):
        super().__init__(columns)
        self.collection = collection
        self.filter = filter
        self.projection = projection
        self.comparison_columns = comparison_columns
        self.unique_columns = unique_columns


class MongoDBSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """The connection configuration class used for MongoDB sources"""
    connection_string: str
    query: str = None
    database: str = None

    def __init__(self, settings: dict):
        if not settings.get("database", None):
            raise Exception("Database name in connection settings is required")

        self.database = settings["database"]

        if settings.get("connection_string", False):
            self.connection_string = settings["connection_string"]
            return

        self.connection_string = "mongodb://"
        f"{settings['username']}:{settings['password']}"
        f"@{settings['host']}:{settings['port']}"

        self.projection = settings["projection"] if settings.get(
            "projection", False) else {}


@register_source("mongodb", MongoDBSourceConfiguration, MongoDBSourceConnectionSettings)
class MongodbSource(SourceInterface):
    ConnectionSettingsClass = MongoDBSourceConnectionSettings
    SourceConfigClass = MongoDBSourceConfiguration

    """ A source for MongoDB data """

    def _configure(self):
        pass

    def _connect(self):
        pass

    def _disconnect(self):
        pass

    def _query(self, params=None, customQuery: str = None, returnData: bool = True) -> DataFrame:
        # TODO: See how this is used from other places to see how to implement this

        if returnData:
            with MongoClient(self.connection_settings.connection_string) as client:
                db = client[self.connection_settings.database]
                collection = db[self.source_configuration.collection]
                return find_polars_all(collection, self.source_configuration.filter, projection=self.source_configuration.projection)

        # TODO: Figure out where and how this is used
        with MongoClient(self.connection_settings.connection_string) as client:
            db = client[self.connection_settings.database]
            collection = db[self.source_configuration.collection]
            find_polars_all(collection, self.source_configuration.filter,
                            projection=self.source_configuration.projection)

    def insert(self, data: DataFrame) -> int:
        return 0

    def update(self, data: DataFrame) -> int:
        return 0

    def delete(self, data: DataFrame) -> int:
        return 0
