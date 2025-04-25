"""Contains the Postgresql source implementation."""

import json
import uuid
from typing import Any

import polars as pl
import psycopg

from ...diff import DiffStats, DiffUpdate
from ..interface import SourceInterface
from ..registrated_source import register_source
from .postgresql_config import PostgresConfig, PostgresConfigArguments
from .postgresql_diff import PostgresDiff, PostgresDiffArguments
from .postgresql_sync import PostgresSync, PostgresSyncArguments


@register_source("Postgresql")
class PostgresSource(SourceInterface):
    """A source for Postgresql data"""

    ConfigArgumentsClass = PostgresConfigArguments
    ConfigClass = PostgresConfig
    SyncArgumentsClass = PostgresSyncArguments
    SyncClass = PostgresSync
    DiffArgumentsClass = PostgresDiffArguments
    DiffClass = PostgresDiff

    diff_config: PostgresDiff = None
    diff_config_arguments: PostgresDiffArguments = None

    def _configure(self):
        pass

    def _connect(self):
        pass

    def _disconnect(self):
        pass

    def _query(
        self, params=None, customQuery: str = None, returnData: bool = True
    ) -> pl.DataFrame:
        query = customQuery

        if query is None:
            if self.source_configuration.query is not None:
                query = self.source_configuration.query

            if query is None:
                if self.source_configuration.table is None:
                    raise ValueError("No query or table specified")

                query = f"SELECT * FROM {self.source_configuration.table}"

        if returnData:
            return pl.read_database_uri(
                query=query,
                uri=self.connection_settings.connection_string,
            )

            # Pylint failes to acnowledge that the context manager is used
            # The changes won't commit if you remove it
            # pylint: disable=not-context-manager
        with psycopg.connect(self.connection_settings.connection_string) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)

    def _insert(
        self, data: pl.DataFrame, table: str = None, connection_string: str = None
    ):
        if table is None:
            table = self.source_configuration.table
        if connection_string is None:
            connection_string = self.connection_settings.connection_string

        connection_string = connection_string.replace(
            "postgresql://", "postgresql+psycopg://"
        )

        data.write_database(table, connection_string, if_table_exists="append")

        return len(data)

    def insert(self, data: pl.DataFrame):
        return self._insert(data)

    def update(self, data: pl.DataFrame):
        self._validate_unique_columns()
        temp_table_name = (
            self.source_configuration.table
            + "_udTemp_"
            + str(uuid.uuid4()).replace("-", "")
        ).lower()

        try:
            # Pylint failes to acnowledge that the context manager is used
            # The changes won't commit if you remove it
            # pylint: disable=not-context-manager
            with psycopg.connect(
                self.connection_settings.connection_string
            ) as connection:
                with connection.cursor() as cursor:
                    create_temp_table_with_same_structure_as_destination = f"CREATE TABLE {temp_table_name} AS SELECT * FROM {self.source_configuration.table} where 1=0"
                    cursor.execute(create_temp_table_with_same_structure_as_destination)

            self._insert(data, table=temp_table_name)

            source_table_name = temp_table_name
            destination_table_name = self.source_configuration.table

            columns_to_update = [
                column.name
                for column in data.get_columns()
                if not column.name in self.source_configuration.unique_columns
                and not column.name in self.source_configuration.skip_columns
            ]
            set_values_of_comparison_columns = "SET " + ", ".join(
                (
                    f"{columnName} = {source_table_name}.{columnName}"
                    for columnName in columns_to_update
                )
            )

            where_unique_columns_are_matching = "WHERE " + " AND ".join(
                (
                    f"{destination_table_name}.{columnName} = {source_table_name}.{columnName}"
                    for columnName in self.source_configuration.unique_columns
                )
            )

            query = f"""
                UPDATE {self.source_configuration.table} AS {destination_table_name}
                {set_values_of_comparison_columns}
                FROM {temp_table_name} AS {source_table_name}
                {where_unique_columns_are_matching}
            """

            self._query(customQuery=query, returnData=False)

            return len(data)
        finally:
            # Pylint failes to acnowledge that the context manager is used
            # The changes won't commit if you remove it
            # pylint: disable=not-context-manager
            with psycopg.connect(
                self.connection_settings.connection_string
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")

    def delete(self, data: pl.DataFrame):
        self._validate_unique_columns()
        batch_size = 500
        batches = [data]

        if len(data) > batch_size:
            batches = []

            for i in range(0, len(data), batch_size):
                batches.append(data[i : i + batch_size])

        for batch in batches:
            id_clause = " AND ".join(
                (
                    f"{columnName} IN ({','.join([self._quote_if_needed(x) for x in batch[columnName].to_list()])})"
                    for columnName in self.source_configuration.unique_columns
                )
            )

            query = f"""
                DELETE FROM {self.source_configuration.table}
                WHERE {id_clause}
            """

            self._query(customQuery=query, returnData=False)

        return len(data)

    def store_diff(self, diff):
        if not self.diff_config:
            raise ValueError("Diff configuration is missing")

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.diff_config.table} (
            uuid UUID PRIMARY KEY,
            name VARCHAR(255),
            date TIMESTAMP,
            version VARCHAR(255),
            updates JSONB,
            inserts JSONB,
            deletes JSONB,
            stats JSONB
        )
        """
        insert_sql = f"""
        INSERT INTO {self.diff_config.table} (uuid, name, date, version, updates, inserts, deletes, stats) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        row_data = (
            str(diff.uuid),
            diff.name,
            diff.date_as_string(),
            diff.version,
            json.dumps(diff.updates, cls=DiffUpdate.JsonEncoder),
            json.dumps(diff.inserts),
            json.dumps(diff.deletes),
            json.dumps(diff.stats, cls=DiffStats.JsonEncoder),
        )

        # Pylint failes to acnowledge that the context manager is used
        # The changes won't commit if you remove it
        # pylint: disable=not-context-manager
        with psycopg.connect(self.connection_settings.connection_string) as connection:
            with connection.cursor() as cursor:
                cursor.execute(create_table_sql)
                cursor.execute(insert_sql, row_data)
                connection.commit()

    def _quote_if_needed(self, identifier: Any) -> str:
        if isinstance(identifier, str):
            return f"'{identifier}'"
        return str(identifier)

    def _validate_unique_columns(self):
        if not self.source_configuration.unique_columns:
            raise ValueError(
                "Unique columns are required for PostgreSQL when used as a destination"
            )
