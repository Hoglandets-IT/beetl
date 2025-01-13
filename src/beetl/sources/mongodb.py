from typing import List
from polars import DataFrame, Object
from .interface import (
    register_source,
    SourceInterface,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings,
)
from pymongo import MongoClient, UpdateOne, DeleteOne


class MongoDBSourceConfiguration(SourceInterfaceConfiguration):
    """The configuration class used for MongoDB sources"""
    collection: str = None
    filter: str = None
    projection: dict = None
    unique_fields: List[str] = None

    def __init__(self, collection: str = None, filter: dict = {}, projection: dict = {}, uniqueFields: List[str] = []):
        super().__init__()
        self.collection = collection
        self.filter = filter
        self.projection = projection
        self.unique_fields = uniqueFields


class MongoDBSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """The connection configuration class used for MongoDB sources"""
    connection_string: str
    query: str = None
    database: str = None

    def get_connection_string_components(self, settings: dict):
        config = {
            "host": settings.get("host", None),
            "port":  settings.get("post", None),
            "username":  settings.get("username", None),
            "password":  settings.get("password", None),
        }
        for _, value in config.items():
            if not value:
                return None

        return config

    def __init__(self, settings: dict):
        self.projection = settings.get("projection", {})

        if not settings.get("database", None):
            raise Exception("Database name in connection settings is required")

        self.database = settings["database"]

        if settings.get("connection_string", False):
            self.connection_string = settings["connection_string"]
        else:
            connection_string_components = self.get_connection_string_components(
                settings)
            if not connection_string_components:
                raise Exception(
                    "Connection string or host, port, username, and password are required")
            self.connection_string = f"mongodb://{connection_string_components['username']}:{connection_string_components['password']}@{connection_string_components['host']}:{connection_string_components['port']}/"


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

    def _query(self, params=None) -> DataFrame:
        with MongoClient(self.connection_settings.connection_string) as client:
            db = client[self.connection_settings.database]
            collection = db[self.source_configuration.collection]
            polar = DataFrame(collection.find(
                self.source_configuration.filter, projection=self.source_configuration.projection))
            if "_id" in polar.columns and polar["_id"].dtype == Object:
                polar = polar.with_columns(
                    polar["_id"].map_elements(lambda oid: str(oid), return_dtype=str))
            return polar

    def insert(self, data: DataFrame) -> int:
        with MongoClient(self.connection_settings.connection_string) as client:
            db = client[self.connection_settings.database]
            collection = db[self.source_configuration.collection]
            result = collection.insert_many(data.to_dicts())

        return len(result.inserted_ids)

    def update(self, data: DataFrame) -> int:
        self._validate_unique_fields()
        updates = []
        for row in data.to_dicts():
            filter = {field_name: row[field_name]
                      for field_name in self.source_configuration.unique_fields}
            for field_name in self.source_configuration.unique_fields:
                del row[field_name]
            updates.append(UpdateOne(filter, {"$set": row}))

        if not len(updates):
            return 0

        with MongoClient(self.connection_settings.connection_string) as client:
            db = client[self.connection_settings.database]
            collection = db[self.source_configuration.collection]
            result = collection.bulk_write(updates)

        return result.modified_count

    def delete(self, data: DataFrame) -> int:
        self._validate_unique_fields()
        deletes = []
        for row in data.to_dicts():
            filter = {field_name: row[field_name]
                      for field_name in self.source_configuration.unique_fields}
            deletes.append(DeleteOne(filter))

        with MongoClient(self.connection_settings.connection_string) as client:
            db = client[self.connection_settings.database]
            collection = db[self.source_configuration.collection]
            result = collection.bulk_write(deletes)

        return result.deleted_count

    def _validate_unique_fields(self):
        if not self.source_configuration.unique_fields:
            raise ValueError(
                "Unique fields are required for MongoDB when used as a destination")
