"""
Definition for the SqlServer source
"""

import json
from typing import Optional
from uuid import uuid4

import pandas as pd
import polars as pl
import pyodbc
import sqlalchemy as sqla

from ...diff import Diff, DiffStats, DiffUpdate
from ..interface import SourceInterface
from ..registrated_source import register_source
from .sqlserver_config import SqlserverConfig, SqlserverConfigArguments
from .sqlserver_diff import SqlserverDiff, SqlserverDiffArguments
from .sqlserver_sync import SqlserverSync, SqlserverSyncArguments


@register_source("Sqlserver")
class SqlserverSource(SourceInterface):
    """
    Class for interacting with SqlServer databases.
    """

    ConfigArgumentsClass = SqlserverConfigArguments
    ConfigClass = SqlserverConfig
    SyncArgumentsClass = SqlserverSyncArguments
    SyncClass = SqlserverSync
    DiffArgumentsClass = SqlserverDiffArguments
    DiffClass = SqlserverDiff

    diff_config_arguments: Optional[SqlserverDiffArguments] = None
    diff_config: Optional[SqlserverDiff] = None
    connection: Optional[pyodbc.Connection] = None
    engine: Optional[sqla.Engine] = None

    def _configure(self):
        pass

    def _connect(self):
        try:
            self.engine = sqla.create_engine(self.connection_settings.connection_string)
        except ModuleNotFoundError:
            try:
                self.engine = sqla.create_engine(
                    self.connection_settings.connection_string.replace(
                        "mssql://", "mssql+pyodbc://"
                    )
                )
            except ModuleNotFoundError:
                self.engine = sqla.create_engine(
                    self.connection_settings.connection_string.replace(
                        "mssql://", "mssql+pymssql://"
                    )
                )
        self.connection = self.engine.connect()

    def _disconnect(self):
        self.connection.commit()
        self.connection.close()

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

                query = f"SELECT * FROM [{self.source_configuration.table}]"

        if returnData:
            pdd = pd.read_sql_query(sql=query, con=self.connection)
            return pl.from_pandas(pdd)

        if isinstance(query, str):
            query = sqla.text(query)
        self.connection.execute(query)

    def _insert(
        self,
        data: pl.DataFrame,
        table: str = None,
        tempDB: bool = False,
    ):
        # Check for binary tables
        data_columns = data.get_columns()
        for column in data_columns:
            if column.dtype == pl.Binary:
                data = data.with_columns(
                    pl.col(column.name).map_elements(
                        lambda x: bytes(x), return_dtype=pl.Binary
                    )
                )

        # Replace empty strings with None
        if self.source_configuration.replace_empty_strings:
            for column_name in data.columns:
                if data[column_name].dtype in (pl.Utf8, pl.String):
                    data = data.with_columns(
                        pl.when(pl.col(column_name).eq(""))
                        .then(None)
                        .otherwise(pl.col(column_name))
                        .alias(column_name)
                    )

        if table is None:
            table = self.source_configuration.table

        pand_df = data.to_pandas()
        try:
            pand_df.to_sql(
                table,
                if_exists="append" if not tempDB else "replace",
                index=False,
                con=self.connection,
            )
        except ModuleNotFoundError:
            data.write_database(
                table,
                connection=self.connection,
                if_exists="append" if not tempDB else "replace",
            )

        return len(data)

    def insert(self, data: pl.DataFrame):
        if len(data) == 0:
            return 0

        return self._insert(data)

    # Help function to handle COLLATE
    def _collate_clause(
        self, column_name: str, schema: dict, left_alias: str, right_alias: str
    ):
        dtype = schema.get(column_name)

        if dtype in (pl.Utf8, pl.String):
            return (
                f"TRY_CONVERT(NVARCHAR, {left_alias}.[{column_name}]) COLLATE DATABASE_DEFAULT = "
                f"TRY_CONVERT(NVARCHAR, {right_alias}.[{column_name}]) COLLATE DATABASE_DEFAULT"
            )
        else:
            return f"{left_alias}.[{column_name}] = " f"{right_alias}.[{column_name}]"

    def update(self, data: pl.DataFrame):
        self._validate_unique_columns()
        if len(data) == 0:
            return 0

        tempDB = self._get_temp_table_name(self.source_configuration.table)

        # Insert data into temporary table
        self._insert(data, table=tempDB, tempDB=True)

        columns_to_update = [
            column_name
            for column_name in data.columns
            if column_name not in self.source_configuration.unique_columns
            and column_name not in self.source_configuration.skip_columns
        ]
        field_spec = ", ".join(
            (
                f"TDEST.[{column_name}] = TTEMP.[{column_name}]"
                for column_name in columns_to_update
            )
        )

        schema = data.schema
        on_clause = " AND ".join(
            self._collate_clause(column_name, schema, "TDEST", "TTEMP")
            for column_name in self.source_configuration.unique_columns
        )

        query = f"""
            UPDATE TDEST
            SET {field_spec}
            FROM {self.source_configuration.table} AS TDEST
            INNER JOIN {tempDB} AS TTEMP ON {on_clause}
        """
        self._query(customQuery=query, returnData=False)
        try:
            self._query(customQuery="DROP TABLE " + tempDB, returnData=False)
        except Exception:
            pass

        return len(data)

    def delete(self, data: pl.DataFrame):
        self._validate_unique_columns()
        if len(data) == 0:
            return 0

        tempDB = self._get_temp_table_name(self.source_configuration.table)
        self._insert(data, table=tempDB, tempDB=True)

        schema = data.schema
        where_clause = " AND ".join(
            (
                self._collate_clause(column_name, schema, "TTEMP", "TDEST")
                for column_name in self.source_configuration.unique_columns
            )
        )

        if self.source_configuration.soft_delete:
            if (
                self.source_configuration.deleted_field is None
                or self.source_configuration.deleted_value is None
            ):
                raise Exception(
                    "Deleted field and value must be specified when using soft delete"
                )

            query = f"""
                UPDATE TDEST
                SET {self.source_configuration.deleted_field} = {self.source_configuration.deleted_value}
                FROM {self.source_configuration.table} AS TDEST
                WHERE EXISTS (
                    SELECT 1/0
                    FROM {tempDB} AS TTEMP
                    WHERE {where_clause}
                ) 
            """
        else:
            query = f"""
                DELETE TDEST
                FROM {self.source_configuration.table} AS TDEST
                WHERE EXISTS (
                    SELECT 1/0
                    FROM {tempDB} AS TTEMP
                    WHERE {where_clause}
                ) 
            """

        self._query(customQuery=query, returnData=False)

        try:
            self._query(customQuery="DROP TABLE " + tempDB, returnData=False)
        except Exception:
            pass

        return len(data)

    def _validate_unique_columns(self):
        if not self.source_configuration.unique_columns:
            raise ValueError(
                "Unique columns must be specified for SQLServer source when used as destination"
            )

    def _get_temp_table_name(self, table_name: str):
        return f"##{table_name}_{str(uuid4()).replace('-', '')}_temp".lower()

    def store_diff(self, diff: Diff):
        if not self.diff_config:
            raise ValueError("Diff configuration is missing")
        metadata = sqla.MetaData()
        table = sqla.Table(
            self.diff_config.table,
            metadata,
            sqla.Column("uuid", sqla.Uuid, primary_key=True),
            sqla.Column("name", sqla.String),
            sqla.Column("date", sqla.DateTime),
            sqla.Column("version", sqla.String),
            sqla.Column("updates", sqla.String),
            sqla.Column("inserts", sqla.String),
            sqla.Column("deletes", sqla.String),
            sqla.Column("stats", sqla.String),
        )
        metadata.create_all(self.connection)

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
        self.connection.execute(insert_statement)
