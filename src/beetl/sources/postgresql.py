import uuid
import polars as pl
from typing import List

import psycopg
from .interface import (
    register_source,
    SourceInterface,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings,
)


class PostgresqlSourceConfiguration(SourceInterfaceConfiguration):
    """The configuration class used for Postgresql sources"""

    unique_columns: List[str] = None
    skip_columns: List[str] = None
    table: str = None
    query: str = None

    def __init__(
        self,
        table: str = None,
        query: str = None,
        uniqueColumns: List[str] = [],
        skipColumns: List[str] = [],
    ):
        super().__init__()
        self.table = table
        self.query = query
        self.unique_columns = uniqueColumns
        self.skip_columns = skipColumns


class PostgresqlSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """The connection configuration class used for Postgresql sources"""

    connection_string: str

    def __init__(self, settings: dict):
        if settings.get("connection_string", False):
            self.connection_string = settings["connection_string"]
            return

        self.connection_string = "postgresql://"
        f"{settings['username']}:{settings['password']}"
        f"@{settings['host']}:{settings['port']}/{settings['database']}"


@register_source(
    "postgresql", PostgresqlSourceConfiguration, PostgresqlSourceConnectionSettings
)
class PostgresqlSource(SourceInterface):
    ConnectionSettingsClass = PostgresqlSourceConnectionSettings
    SourceConfigClass = PostgresqlSourceConfiguration

    """ A source for Postgresql data """

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
                    raise Exception("No query or table specified")

                query = f"SELECT * FROM {self.source_configuration.table}"

        if returnData:
            return pl.read_database_uri(
                query=query,
                uri=self.connection_settings.connection_string,
            )

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
            "postgresql://", "postgresql+psycopg://")

        data.write_database(
            table, connection_string, if_table_exists="append"
        )

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
            with psycopg.connect(
                self.connection_settings.connection_string
            ) as connection:
                with connection.cursor() as cursor:
                    create_temp_table_with_same_structure_as_destination = f"CREATE TABLE {temp_table_name} AS SELECT * FROM {self.source_configuration.table} where 1=0"
                    cursor.execute(
                        create_temp_table_with_same_structure_as_destination)

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
                batches.append(data[i: i + batch_size])

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

    def _quote_if_needed(self, id: any) -> str:
        if isinstance(id, str):
            return f"'{id}'"
        return str(id)

    def _validate_unique_columns(self):
        if not self.source_configuration.unique_columns:
            raise ValueError(
                "Unique columns are required for PostgreSQL when used as a destination"
            )
