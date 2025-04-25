"""A source for MySQL data"""

import json
from typing import Any

import polars as pl
import sqlalchemy as sqla

from ...diff import DiffStats, DiffUpdate
from ..interface import SourceInterface
from ..registrated_source import register_source
from .mysql_config import MysqlConfig, MysqlConfigArguments
from .mysql_diff import MysqlDiff, MysqlDiffArguments
from .mysql_sync import MysqlSync, MysqlSyncArguments


@register_source("Mysql")
class MysqlSource(SourceInterface):
    """A source for MySQL data"""

    ConfigArgumentsClass = MysqlConfigArguments
    ConfigClass = MysqlConfig
    SyncArgumentsClass = MysqlSyncArguments
    SyncClass = MysqlSync
    DiffArgumentsClass = MysqlDiffArguments
    DiffClass = MysqlDiff

    diff_config: MysqlDiff = None
    diff_config_arguments: MysqlDiffArguments = None

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
            try:
                df = pl.read_database_uri(
                    query=query,
                    uri=self.connection_settings.connection_string,
                )
            except ModuleNotFoundError:
                df = pl.read_database_uri(
                    query=query,
                    uri=self.connection_settings.connection_string.replace(
                        "mysql://", "mysql+pymysql://"
                    ),
                )

            return df
        try:
            with sqla.create_engine(
                self.connection_settings.connection_string
            ).connect() as con:
                con.execute(query)
                con.commit()

        except ModuleNotFoundError:
            with sqla.create_engine(
                self.connection_settings.connection_string.replace(
                    "mysql://", "mysql+pymysql://"
                )
            ).connect() as con:
                con.execute(sqla.text(query))
                con.commit()

    def _insert(
        self, data: pl.DataFrame, table: str = None, connection_string: str = None
    ):
        if table is None:
            table = self.source_configuration.table
        if connection_string is None:
            connection_string = self.connection_settings.connection_string

        try:
            data.write_database(
                table,
                self.connection_settings.connection_string,
                if_table_exists="append",
            )
        except ModuleNotFoundError:
            data.write_database(
                table,
                self.connection_settings.connection_string.replace(
                    "mysql://", "mysql+pymysql://"
                ),
                if_table_exists="append",
            )

        return len(data)

    def insert(self, data: pl.DataFrame):
        self._validate_unique_columns()
        return self._insert(data)

    def update(self, data: pl.DataFrame):
        self._validate_unique_columns()

        tempDB = (self.source_configuration.table + "_udTemp").lower()
        try:
            with sqla.create_engine(
                self.connection_settings.connection_string
            ).connect() as con:
                con.execute(sqla.text(f"DROP TABLE IF EXISTS {tempDB}"))
                con.execute(
                    sqla.text(
                        f"CREATE TEMPORARY TABLE {tempDB} "
                        f"LIKE {self.source_configuration.table}"
                    )
                )
                con.commit()

        except ModuleNotFoundError:
            with sqla.create_engine(
                self.connection_settings.connection_string.replace(
                    "mysql://", "mysql+pymysql://"
                )
            ).connect() as con:
                con.execute(sqla.text(f"DROP TABLE IF EXISTS {tempDB}"))
                con.execute(
                    sqla.text(
                        f"CREATE TEMPORARY TABLE {tempDB} "
                        f"LIKE {self.source_configuration.table}"
                    )
                )
                con.commit()

        # Insert data into temporary table
        self._insert(data, table=tempDB)

        fields_to_update = [
            field.name
            for field in data.get_columns()
            if not field.name in self.source_configuration.skip_columns
            or not field.name in self.source_configuration.unique_columns
        ]

        field_spec = ", ".join((f"`{fieldName}`" for fieldName in fields_to_update))

        query = f"""
            REPLACE INTO {self.source_configuration.table} ({field_spec})
            SELECT {field_spec} FROM {tempDB}
        """

        self._query(customQuery=query, returnData=False)

        return len(data)

    def delete(self, data: pl.DataFrame):
        batch_size = 500
        batches = [data]

        if len(data) > batch_size:
            batches = []

            for i in range(0, len(data), batch_size):
                batches.append(data[i : i + batch_size])

        for batch in batches:
            id_clause = " AND ".join(
                (
                    f"`{fieldName}` IN ({','.join([self._quote_if_needed(x) for x in batch[fieldName].to_list()])})"
                    for fieldName in self.source_configuration.unique_columns
                )
            )

            query = f"""
                DELETE FROM {self.source_configuration.table}
                WHERE {id_clause}
            """

            self._query(customQuery=query, returnData=False)

        return len(data)

    def _validate_unique_columns(self):
        if not self.source_configuration.unique_columns:
            raise ValueError(
                "MySQL source requires the unique_columns to be set if used as a destination"
            )

    def _quote_if_needed(self, identifier: Any) -> str:
        if isinstance(identifier, str):
            return f"'{identifier}'"
        return str(identifier)

    def store_diff(self, diff):
        if not self.diff_config:
            raise ValueError("Diff configuration is missing")
        metadata = sqla.MetaData()
        table = sqla.Table(
            self.diff_config.table,
            metadata,
            sqla.Column("uuid", sqla.Uuid, primary_key=True),
            sqla.Column("name", sqla.String(length=255)),
            sqla.Column("date", sqla.DateTime),
            sqla.Column("version", sqla.String(length=64)),
            sqla.Column("updates", sqla.TEXT),
            sqla.Column("inserts", sqla.TEXT),
            sqla.Column("deletes", sqla.TEXT),
            sqla.Column("stats", sqla.TEXT),
        )

        insert_statement = sqla.insert(table).values(
            name=diff.name,
            date=diff.date,
            uuid=diff.uuid,
            version=diff.version,
            updates=json.dumps(diff.updates, cls=DiffUpdate.JsonEncoder),
            inserts=json.dumps(diff.inserts),
            deletes=json.dumps(diff.deletes),
            stats=json.dumps(diff.stats, cls=DiffStats.JsonEncoder),
        )
        with sqla.create_engine(
            self.connection_settings.connection_string.replace(
                "mysql://", "mysql+pymysql://"
            )
        ).connect() as con:
            metadata.create_all(con)
            con.execute(insert_statement)
            con.commit()
