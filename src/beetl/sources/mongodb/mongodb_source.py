"""Source for communicating with MongoDB"""

import json
from typing import Optional

from polars import DataFrame, Object
from pymongo import DeleteOne, MongoClient, UpdateOne

from ...diff.diff_model import DiffStats, DiffUpdate
from ..interface import SourceInterface
from ..registrated_source import register_source
from .mongodb_config import MongodbConfig, MongodbConfigArguments
from .mongodb_diff import MongodbDiff, MongodbDiffArguments
from .mongodb_sync import MongodbSync, MongodbSyncArguments


@register_source("Mongodb")
class MongodbSource(SourceInterface):
    """Source for communicating with MongoDB"""

    ConfigArgumentsClass = MongodbConfigArguments
    ConfigClass = MongodbConfig
    SyncArgumentsClass = MongodbSyncArguments
    SyncClass = MongodbSync
    DiffArgumentsClass = MongodbDiffArguments
    DiffClass = MongodbDiff

    diff_config_arguments: Optional[MongodbDiffArguments] = None
    diff_config: Optional[MongodbDiff] = None

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
                    polar["_id"].map_elements(str, return_dtype=str)
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
            mutation_filter = {
                field_name: row[field_name]
                for field_name in self.source_configuration.unique_fields
            }
            for field_name in self.source_configuration.unique_fields:
                del row[field_name]
            updates.append(UpdateOne(mutation_filter, {"$set": row}))

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
            mutation_filter = {
                field_name: row[field_name]
                for field_name in self.source_configuration.unique_fields
            }
            deletes.append(DeleteOne(mutation_filter))

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

    def store_diff(self, diff):
        with MongoClient(self.connection_settings.connection_string) as client:
            db = client[self.connection_settings.database]
            collection = db[self.diff_config.collection]
            result = collection.insert_many(
                [
                    {
                        "name": diff.name,
                        "date": diff.date_as_string(),
                        "uuid": str(diff.uuid),
                        "version": diff.version,
                        "updates": [update.to_dict() for update in diff.updates],
                        "inserts": diff.inserts,
                        "deletes": diff.deletes,
                        "stats": diff.stats.to_dict(),
                    }
                ]
            )
            if not len(result.inserted_ids) == 1:
                raise ValueError("Error inserting diff into MongoDB")
