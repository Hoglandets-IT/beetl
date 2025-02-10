import os

import pandas as pd
import polars as pl

from ..interface import SourceInterface
from ..registrated_source import register_source
from .xml_config import XmlConfig, XmlConfigArguments
from .xml_sync import XmlSync, XmlSyncArguments


@register_source("Xml")
class XmlSource(SourceInterface):
    """A source for reading and writing XML files"""

    ConfigArgumentsClass = XmlConfigArguments
    ConfigClass = XmlConfig
    SyncArgumentsClass = XmlSyncArguments
    SyncClass = XmlSync

    connection_settings: XmlConfig = None
    source_configuration: XmlSync = None
    mutation_data: pl.DataFrame = pl.DataFrame()

    def _configure(self):
        pass

    def _connect(self):
        pass

    def _disconnect(self):
        """Flushes the mutated dataframe to the xml file"""
        self.flush_data_to_file(self.mutation_data)

    def _query(self, params=None) -> pl.DataFrame:
        """Reads the XML file and returns a polars DataFrame"""

        file_does_not_exist = not os.path.exists(self.connection_settings.path)
        if file_does_not_exist:
            return pl.DataFrame()

        file_is_empty = os.path.getsize(self.connection_settings.path) == 0
        if file_is_empty:
            return pl.DataFrame()

        data = pl.from_pandas(
            pd.read_xml(
                self.connection_settings.path,
                encoding=self.connection_settings.encoding,
                parser=self.source_configuration.parser,
                xpath=self.source_configuration.xpath,
                dtype=self.source_configuration.types,
                stylesheet=self.source_configuration.xsl,
            )
        )

        return data

    def insert(self, data: pl.DataFrame):
        """Xml files are not yet supported as a destination"""
        unique_columns = self.get_unique_columns()
        destination = self.load_data_for_mutation(data)
        updated = destination.update(
            data, on=unique_columns, how="full", include_nulls=True
        )
        self.save_data_after_mutation(updated)
        return len(data)

    def update(self, data: pl.DataFrame):
        """Xml files are not yet supported as a destination"""
        unique_columns = self.get_unique_columns()
        destination = self.load_data_for_mutation(data)
        updated = destination.update(
            data, on=unique_columns, how="left", include_nulls=True
        )
        self.save_data_after_mutation(updated)
        return len(data)

    def delete(self, data: pl.DataFrame):
        """Xml files are not yet supported as a destination"""
        unique_columns = self.get_unique_columns()
        destination = self.load_data_for_mutation(data)
        updated = destination.join(data, on=unique_columns, how="anti")
        self.save_data_after_mutation(updated)
        return len(data)

    def load_data_for_mutation(self, data_schema: pl.DataFrame) -> pl.DataFrame:
        if self.mutation_data.height > 0:
            return self.mutation_data

        mutation_data = self._query()
        self.mutation_data = self.append_missing_columns(data_schema, mutation_data)

        return self.mutation_data

    def save_data_after_mutation(self, data: pl.DataFrame) -> None:
        self.mutation_data = data

    def flush_data_to_file(self, data: pl.DataFrame) -> None:
        data_frame_is_missing_columns = data.width == 0
        if data_frame_is_missing_columns:
            return

        if not os.path.exists(self.connection_settings.path):
            with open(self.connection_settings.path, "w") as file:
                file.flush()

        data.to_pandas().to_xml(
            self.connection_settings.path,
            encoding=self.connection_settings.encoding,
            root_name=self.source_configuration.root_name,
            row_name=self.source_configuration.row_name,
            index=False,
            parser="etree",
        )

    def get_unique_columns(self):
        if not self.source_configuration.unique_columns:
            raise ValueError(
                "Unique columns must be defined in the configuration when used as a destination"
            )

        return self.source_configuration.unique_columns

    def append_missing_columns(
        self, source: pl.DataFrame, destination: pl.DataFrame
    ) -> pl.DataFrame:
        columns_to_append = [
            column for column in source.columns if column not in destination.columns
        ]
        for column in columns_to_append:
            destination = destination.with_columns(
                pl.Series(
                    column, [None] * destination.height, dtype=source[column].dtype
                )
            )

        return destination
