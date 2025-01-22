import polars as pl
from pydantic import BaseModel, ConfigDict, Field, model_validator
import sqlalchemy as sqla
from typing import Annotated, List, Optional
from .interface import (
    SourceInterfaceArguments,
    register_source,
    SourceInterface,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings,
)


class MysqlSourceConfigurationArguments(BaseModel):
    model_config = ConfigDict(extra='forbid')

    table: Annotated[Optional[str], Field(
        default=None, description="Mandatory if configured as a destination, and if query isn't provided")]
    query: Annotated[Optional[str], Field(default=None)]
    uniqueColumns: Annotated[List[str], Field(default=[])]
    skipColumns: Annotated[List[str], Field(default=[])]


class MysqlSourceConfiguration(SourceInterfaceConfiguration):
    """The configuration class used for MySQL sources"""

    unique_columns: List[str] = None
    skip_columns: List[str] = None
    table: str = None
    query: str = None

    def __init__(self, arguments: MysqlSourceConfigurationArguments):
        super().__init__(arguments)

        self.table = arguments.table
        self.query = arguments.query
        self.unique_columns = arguments.uniqueColumns
        self.skip_columns = arguments.skipColumns


class MysqlSourceArguments(SourceInterfaceArguments):
    class MysqlConnectionArguments(BaseModel):
        model_config = ConfigDict(extra='forbid')

        connection_string: Annotated[Optional[str], Field(default=None)]
        username: Annotated[Optional[str], Field(default=None)]
        password: Annotated[Optional[str], Field(default=None)]
        host: Annotated[Optional[str], Field(default=None)]
        port: Annotated[Optional[str], Field(default=None)]
        database: Annotated[Optional[str], Field(default=None)]

        @ model_validator(mode="after")
        def validate_connection_string_or_components(cls, instance: "MysqlSourceArguments.MySqlConnectionArguments"):
            connection_string_is_not_present = not instance.connection_string
            connection_string_components = [
                "host", "port", "username", "password", "database"]
            if connection_string_is_not_present:
                dict = instance.model_dump()
                for component in connection_string_components:
                    if not dict.get(component, None):
                        raise ValueError(
                            f"'{component}' is missing. {connection_string_components} are required if 'connection_string' is not provided")
            return instance

    type: Annotated[str, Field(default="Mysql")] = "Mysql"
    connection: MysqlConnectionArguments


class MysqlSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """The connection configuration class used for MySQL sources"""

    connection_string: str
    query: str = None
    table: str = None

    def __init__(self, arguments: MysqlSourceArguments):
        super().__init__(arguments)

        connection_string: str
        if arguments.connection.connection_string:
            connection_string = arguments.connection.connection_string
        if not arguments.connection.connection_string:
            connection_string = "mysql+pymysql://"
            f"{arguments.connection.username}:{arguments.connection.password}"
            f"@{arguments.connection.host}:{arguments.connection.port}/{arguments.connection.database}"

        self.connection_string = connection_string


@register_source("Mysql", MysqlSourceConfiguration, MysqlSourceConnectionSettings)
class MysqlSource(SourceInterface):
    ConnectionSettingsArguments = MysqlSourceArguments
    ConnectionSettingsClass = MysqlSourceConnectionSettings
    SourceConfigArguments = MysqlSourceConfigurationArguments
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
                table, self.connection_settings.connection_string, if_table_exists="append"
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

        fields_to_update = [field.name for field in data.get_columns(
        ) if not field.name in self.source_configuration.skip_columns or not field.name in self.source_configuration.unique_columns]

        field_spec = ", ".join(
            (f"`{fieldName}`" for fieldName in fields_to_update))

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
                batches.append(data[i: i + batch_size])

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
                "MySQL source requires the unique_columns to be set if used as a destination")

    def _quote_if_needed(self, id: any) -> str:
        if isinstance(id, str):
            return f"'{id}'"
        return str(id)
