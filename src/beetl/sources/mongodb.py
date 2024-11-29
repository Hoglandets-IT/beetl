from typing import List
from polars import DataFrame, Object
from .interface import (
    ColumnDefinition,
    register_source,
    SourceInterface,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings,
)
from pymongo import MongoClient, UpdateOne, DeleteOne


class MongoDBSourceConfiguration(SourceInterfaceConfiguration):
    """The configuration class used for MongoDB sources"""
    columns: List[ColumnDefinition]
    collection: str = None
    filter: str = None
    projection: dict = None

    def __init__(self, columns: list, collection: str = None, filter: dict = {}, projection: dict = {}):
        super().__init__(columns)
        self.collection = collection
        self.filter = filter
        self.projection = projection


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

    # TODO: Figure out where and how these params are used
    def _query(self, params=None, customQuery: str = None, returnData: bool = True) -> DataFrame:

        if returnData:
            with MongoClient(self.connection_settings.connection_string) as client:
                db = client[self.connection_settings.database]
                collection = db[self.source_configuration.collection]
                polar = DataFrame(collection.find(
                    self.source_configuration.filter, projection=self.source_configuration.projection))
                if "_id" in polar.columns and polar["_id"].dtype == Object:
                    polar = polar.with_columns(
                        polar["_id"].map_elements(lambda oid: str(oid)))
                return polar

        # TODO: Figure out where and how this is used, if not, remove it
        with MongoClient(self.connection_settings.connection_string) as client:
            db = client[self.connection_settings.database]
            collection = db[self.source_configuration.collection]
            collection.find(
                self.source_configuration.filter, projection=self.source_configuration.projection)

    def insert(self, data: DataFrame) -> int:
        with MongoClient(self.connection_settings.connection_string) as client:
            db = client[self.connection_settings.database]
            collection = db[self.source_configuration.collection]
            result = collection.insert_many(data.to_dicts())

        return len(result.inserted_ids)

    def update(self, data: DataFrame) -> int:
        updates = []
        for row in data.to_dicts():
            filter = {key: row[key]
                      for key in self.source_configuration.unique_columns}
            for key in self.source_configuration.unique_columns:
                del row[key]
            updates.append(UpdateOne(filter, {"$set": row}))

        if not len(updates):
            return 0

        with MongoClient(self.connection_settings.connection_string) as client:
            db = client[self.connection_settings.database]
            collection = db[self.source_configuration.collection]
            result = collection.bulk_write(updates)

        return result.modified_count

    def delete(self, data: DataFrame) -> int:
        deletes = []
        for row in data.to_dicts():
            filter = {key: row[key]
                      for key in self.source_configuration.unique_columns}
            deletes.append(DeleteOne(filter))

        with MongoClient(self.connection_settings.connection_string) as client:
            db = client[self.connection_settings.database]
            collection = db[self.source_configuration.collection]
            result = collection.bulk_write(deletes)

        return result.deleted_count
