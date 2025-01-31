from polars import DataFrame, Object
from pymongo import DeleteOne, MongoClient, UpdateOne

from ..interface import SourceInterface
from ..registrated_source import register_source
from .mongodb_config import MongodbConfig, MongodbConfigArguments
from .mongodb_sync import MongodbSync, MongodbSyncArguments


@register_source("Mongodb")
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
            polar = DataFrame(
                collection.find(
                    self.source_configuration.filter,
                    projection=self.source_configuration.projection,
                )
            )
            if "_id" in polar.columns and polar["_id"].dtype == Object:
                polar = polar.with_columns(
                    polar["_id"].map_elements(lambda oid: str(oid), return_dtype=str)
                )
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
            filter = {
                field_name: row[field_name]
                for field_name in self.source_configuration.unique_fields
            }
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
            filter = {
                field_name: row[field_name]
                for field_name in self.source_configuration.unique_fields
            }
            deletes.append(DeleteOne(filter))

        with MongoClient(self.connection_settings.connection_string) as client:
            db = client[self.connection_settings.database]
            collection = db[self.source_configuration.collection]
            result = collection.bulk_write(deletes)

        return result.deleted_count

    def _validate_unique_fields(self):
        if not self.source_configuration.unique_fields:
            raise ValueError(
                "Unique fields are required for MongoDB when used as a destination"
            )
