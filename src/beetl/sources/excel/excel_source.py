"""A source for excel data"""

import json
import os
from pathlib import Path

import polars as pl

from ...diff.diff_model import Diff, DiffStats, DiffUpdate
from ..interface import SourceInterface
from ..registrated_source import register_source
from .excel_config import ExcelConfig, ExcelConfigArguments
from .excel_diff import ExcelDiff, ExcelDiffArguments
from .excel_sync import ExcelSync, ExcelSyncArguments


@register_source("Excel")
class ExcelSource(SourceInterface):
    """A source for excel data"""

    ConfigArgumentsClass = ExcelConfigArguments
    ConfigClass = ExcelConfig
    DiffArgumentsClass = ExcelDiffArguments
    DiffClass = ExcelDiff
    SyncClass = ExcelSync
    SyncArgumentsClass = ExcelSyncArguments

    connection_settings_arguments: ExcelConfigArguments = None
    connection_settings: ExcelConfig = None
    source_configuration_arguments: ExcelSyncArguments = None
    source_configuration: ExcelSync = None

    def _configure(self):
        pass

    def _connect(self):
        pass

    def _disconnect(self):
        pass

    def _query(self, params=None) -> pl.DataFrame:
        return pl.read_excel(
            self.connection_settings.path,
            schema_overrides=self.source_configuration.types,
        )

    def insert(self, data: pl.DataFrame):
        print("Inserting data into static source...")
        print(data)
        return len(data)

    def update(self, data: pl.DataFrame):
        print("Updating data in static source...")
        print(data)
        return len(data)

    def delete(self, data: pl.DataFrame):
        print("Deleting data from static source")
        print(data)
        return len(data)

    def store_diff(self, diff: Diff):
        existing_data = pl.DataFrame()

        if not os.path.exists(self.connection_settings.path):
            os.makedirs(os.path.dirname(self.connection_settings.path), exist_ok=True)
            Path(os.path.join(self.connection_settings.path)).touch()

        try:
            existing_data = pl.read_excel(self.connection_settings.path)
        # Broad catch is ok here since we just want to figure out if we need to create the file
        # pylint: disable=broad-exception-caught
        except Exception:
            # Do nothing, just replace the file
            pass

        new_data = pl.DataFrame(
            {
                "uuid": str(diff.uuid),
                "name": diff.name,
                "date": diff.date_as_string(),
                "version": diff.version,
                "updates": json.dumps(diff.updates, cls=DiffUpdate.JsonEncoder),
                "inserts": json.dumps(diff.inserts),
                "deletes": json.dumps(diff.deletes),
                "stats": json.dumps(diff.stats, cls=DiffStats.JsonEncoder),
            }
        )

        if existing_data.is_empty():
            existing_data = new_data
        else:
            existing_data = pl.concat([existing_data, new_data])

        existing_data.write_excel(self.connection_settings.path)
