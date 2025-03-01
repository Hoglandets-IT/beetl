from uuid import uuid4

import pandas as pd
import polars as pl
import pyodbc
import sqlalchemy as sqla

from ..interface import SourceInterface
from ..registrated_source import register_source
from .sqlserver_config import SqlserverConfig, SqlserverConfigArguments
from .sqlserver_sync import SqlserverSync, SqlserverSyncArguments


@register_source("Sqlserver")
class SqlserverSource(SourceInterface):
    ConfigArgumentsClass = SqlserverConfigArguments
    ConfigClass = SqlserverConfig
    SyncArgumentsClass = SqlserverSyncArguments
    SyncClass = SqlserverSync

    connection: pyodbc.Connection = None

    """ A source for SqlServer data """

    def _configure(self):
        pass

    def _connect(self):
        try:
            engine = sqla.create_engine(self.connection_settings.connection_string)
        except ModuleNotFoundError:
            try:
                engine = sqla.create_engine(
                    self.connection_settings.connection_string.replace(
                        "mssql://", "mssql+pyodbc://"
                    )
                )
            except ModuleNotFoundError:
                engine = sqla.create_engine(
                    self.connection_settings.connection_string.replace(
                        "mssql://", "mssql+pymssql://"
                    )
                )
        self.connection = engine.connect()

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

        on_clause = " AND ".join(
            (
                f"TDEST.[{column_name}] = TTEMP.[{column_name}]"
                for column_name in self.source_configuration.unique_columns
            )
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

        where_clause = " AND ".join(
            (
                f"TTEMP.[{column_name}] = TDEST.[{column_name}]"
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
