from typing import List
from bson import ObjectId
import pandas
from polars import DataFrame, from_pandas, from_dict
from .interface import (
    ColumnDefinition,
    register_source,
    SourceInterface,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings,
)
from pymongoarrow.schema import Schema
from pymongoarrow.api import find_polars_all, find_pandas_all, find_arrow_all
from pymongo import MongoClient
from pandas import json_normalize
import polars


class MongoDBSourceConfiguration(SourceInterfaceConfiguration):
    """The configuration class used for MongoDB sources"""
    columns: List[ColumnDefinition]
    unique_columns: List[str] = None
    skip_columns: List[str] = None
    collection: str = None
    query: str = None

    def __init__(self, columns: list, collection: str = None, query: str = None):
        super().__init__(columns)
        # TODO: Figure out how to use this
        self.collection = collection
        self.query = query


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
        filter = customQuery if customQuery is not None else {}

        # Projection is used to filter out the fields that are not needed
        # The internal _id field is always returned unless explicitly excluded
        # Include the _id field in the projection only if it's included in the configuration
        projection = {"_id": 0}
        for col in self.source_configuration.columns:
            projection[col.name] = 1

        if returnData:
            with MongoClient(self.connection_settings.connection_string) as client:
                db = client[self.connection_settings.database]
                collection = db[self.source_configuration.collection]
                flattened_data = find_arrow_all(
                    collection, filter, projection=projection).flatten().to_pydict()
                return polars.from_dict(flattened_data)

        # TODO: Figure out where and how this is used
        find_arrow_all(collection, filter, projection=projection)

    def insert(self, data: DataFrame):
        pass

    def update(self, data: DataFrame):
        pass

    def delete(self, data: DataFrame):
        pass
