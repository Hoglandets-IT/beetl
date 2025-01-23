from typing import Annotated, Any, List, Literal, Optional
from polars import DataFrame, Object
from pydantic import BaseModel, ConfigDict, Field, model_validator
from .interface import (
    SourceSyncArguments,
    SourceConfigArguments,
    register_source,
    SourceInterface,
    SourceSync,
    SourceConfig,
)
from pymongo import MongoClient, UpdateOne, DeleteOne


class MongodbSyncArguments(SourceSyncArguments):
    collection: str
    filter: Annotated[Optional[dict[str, Any]], Field(default={})]
    projection: Annotated[Optional[dict[str, int]], Field(default={})]
    uniqueFields: Annotated[Optional[List[str]], Field(
        default=[], description="Required when used as a destination")]


class MongodbSync(SourceSync):
    """The configuration class used for MongoDB sources"""
    collection: Annotated[Optional[str], Field(default=None)]
    filter: Annotated[str, Field(default={})]
    projection: dict = None
    unique_fields: List[str] = None

    def __init__(self, arguments: MongodbSyncArguments):
        super().__init__(arguments)
        self.collection = arguments.collection
        self.filter = arguments.filter
        self.projection = arguments.projection
        self.unique_fields = arguments.uniqueFields


class MongodbConfigArguments(SourceConfigArguments):
    class MongoDBConnectionArguments(BaseModel):
        model_config = ConfigDict(extra='forbid')

        connection_string: Annotated[Optional[str],
                                     Field(min_length=1, default=None)]
        host: Annotated[Optional[str], Field(min_length=1, default=None)]
        port: Annotated[Optional[str], Field(min_length=1, default=None)]
        username: Annotated[Optional[str], Field(min_length=1, default=None)]
        password: Annotated[Optional[str], Field(min_length=1, default=None)]
        database: Annotated[str, Field(min_length=1)]

        @ model_validator(mode="after")
        def validate_connection_string_or_components(cls, instance: "MongodbConfigArguments.MongoDBConnectionArguments"):
            connection_string_is_not_present = not instance.connection_string
            connection_string_components = [
                "host", "port", "username", "password"]
            if connection_string_is_not_present:
                dict = instance.model_dump()
                for component in connection_string_components:
                    if not dict.get(component, None):
                        raise ValueError(
                            f"'{component}' is missing. {connection_string_components} are required if 'connection_string' is not provided")

            return instance

    type: Annotated[Literal["Mongodb"], Field(default="Mongodb")] = "Mongodb"
    connection: MongoDBConnectionArguments


class MongodbConfig(SourceConfig):
    """The connection configuration class used for MongoDB sources"""
    connection_string: str
    query: Optional[str] = None
    database: str

    def __init__(self, arguments: MongodbConfigArguments):
        super().__init__(arguments)

        connection_string: str
        if arguments.connection.connection_string:
            connection_string = arguments.connection.connection_string
        else:
            connection_string = f"mongodb://{arguments.connection.username}:{arguments.connection.password}@{arguments.connection.host}:{arguments.connection.port}/"

        self.connection_string = connection_string
        self.database = arguments.connection.database


@ register_source("Mongodb")
class MongodbSource(SourceInterface):
    ConfigArgumentsClass = MongodbConfigArguments
    ConfigClass = MongodbConfig
    SyncArgumentsClass = MongodbSyncArguments
    SyncClass = MongodbSync

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
