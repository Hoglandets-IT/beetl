import polars as pl
import sqlalchemy as sqla
from typing import List
from .interface import (
    register_source,
    SourceInterface,
    ColumnDefinition,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings,
)


class MysqlSourceConfiguration(SourceInterfaceConfiguration):
    """The configuration class used for MySQL sources"""

    columns: List[ColumnDefinition]
    unique_columns: List[str] = None
    skip_columns: List[str] = None
    table: str = None
    query: str = None

    def __init__(self, columns: list, table: str = None, query: str = None):
        super().__init__(columns)
        self.table = table
        self.query = query


class MysqlSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """The connection configuration class used for MySQL sources"""

    connection_string: str
    query: str = None
    table: str = None

    def __init__(self, settings: dict):
        if settings.get("connection_string", False):
            self.connection_string = settings["connection_string"]
            return

        self.connection_string = "mysql+pymysql://"
        f"{settings['username']}:{settings['password']}"
        f"@{settings['host']}:{settings['port']}/{settings['database']}"


@register_source("mysql", MysqlSourceConfiguration, MysqlSourceConnectionSettings)
class MysqlSource(SourceInterface):
    ConnectionSettingsClass = MysqlSourceConnectionSettings
    SourceConfigClass = MysqlSourceConfiguration

    """ A source for MySQL data """

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

                cols = ",".join(
                    ["`" + col.name + "`" for col in self.source_configuration.columns]
                )

                query = f"SELECT {cols} FROM {self.source_configuration.table}"

        if returnData:
            try:
                df = pl.read_sql(
                    query=query,
                    connection_uri=self.connection_settings.connection_string,
                )
            except ModuleNotFoundError:
                df = pl.read_sql(
                    query=query,
                    connection_uri=self.connection_settings.connection_string.replace(
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
                table, self.connection_settings.connection_string, if_exists="append"
            )
        except ModuleNotFoundError:
            data.write_database(
                table,
                self.connection_settings.connection_string.replace(
                    "mysql://", "mysql+pymysql://"
                ),
                if_exists="append",
            )

        return len(data)

    def insert(self, data: pl.DataFrame):
        return self._insert(data)

    def update(self, data: pl.DataFrame):
        tempDB = self.source_configuration.table + "_udTemp"
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

        field_spec = ", ".join(
            (
                f"`{fld.name}`"
                for fld in self.source_configuration.columns
                if (not fld.skip_update or fld.unique)
            )
        )

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
                    f"`{fld.name}` IN ({','.join([str(x) for x in batch[fld.name].to_list()])})"
                    for fld in self.source_configuration.columns
                    if fld.unique
                )
            )

            query = f"""
                DELETE FROM {self.source_configuration.table}
                WHERE {id_clause}
            """

            self._query(customQuery=query, returnData=False)

        return len(data)
