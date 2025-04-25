"""Contains the CsvSource class for handling CSV data sources."""

import json
import os
from pathlib import Path

import polars as pl

from ...diff import Diff, DiffStats, DiffUpdate
from ..interface import SourceInterface
from ..registrated_source import register_source
from .csv_config import CsvConfig, CsvConfigArguments
from .csv_diff import CsvDiff, CsvDiffArguments


@register_source("Csv")
class CsvSource(SourceInterface):
    """Source for interacting with CSV files."""

    ConfigArgumentsClass = CsvConfigArguments
    ConfigClass = CsvConfig
    DiffArgumentsClass = CsvDiffArguments
    DiffClass = CsvDiff

    diff_config_arguments: CsvDiffArguments = None
    diff_config: CsvDiff = None

    def _configure(self):
        pass

    def _connect(self):
        pass

    def _disconnect(self):
        pass

    def _query(self, params=None) -> pl.DataFrame:
        return pl.read_csv(
            self.connection_settings.path, encoding=self.connection_settings.encoding
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
        if not self.diff_config:
            raise ValueError("Diff configuration is missing")

        existing_data = pl.DataFrame()

        if not os.path.exists(self.connection_settings.path):
            os.makedirs(os.path.dirname(self.connection_settings.path), exist_ok=True)
            Path(os.path.join(self.connection_settings.path)).touch()

        try:
            existing_data = pl.read_csv(
                self.connection_settings.path,
                encoding=self.connection_settings.encoding,
            )
        # Ok to catch broadly here since we are just figuring out if we should replace the file or not.
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

        existing_data.write_csv(self.connection_settings.path)
