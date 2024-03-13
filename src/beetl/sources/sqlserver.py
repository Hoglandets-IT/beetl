import polars as pl
import pandas as pd
import sqlalchemy as sqla
from sqlalchemy.exc import ProgrammingError
import pyodbc
from .interface import (
    register_source,
    SourceInterface,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings,
)


class SqlserverConfiguration(SourceInterfaceConfiguration):
    """The configuration class used for SQLServer sources"""

    table: str = None
    query: str = None
    soft_delete: bool = False
    deleted_field: str = None
    deleted_value: str = "'true'"

    def __init__(self, columns: list, table: str = None, query: str = None, soft_delete: bool = False, deleted_field: str = None, deleted_value: str = "true"):
        super().__init__(columns)
        self.table = table
        self.query = query
        self.soft_delete = soft_delete
        self.deleted_field = deleted_field
        self.deleted_value = deleted_value


class SqlserverConnectionSettings(SourceInterfaceConnectionSettings):
    """The connection configuration class used for SQLServer sources"""

    data: pl.DataFrame
    connection_string: str
    query: str = None
    table: str = None

    def __init__(self, settings: dict):
        if settings.get("connection_string", False):
            self.connection_string = settings["connection_string"]
            if "driver" not in self.connection_string:
                if len(pyodbc.drivers()) == 0:
                    raise pyodbc.DatabaseError(
                        "No ODBC drivers found for SQL Server/pyodbc. "
                        "Please make sure at least one is installed"
                    )
                print("Found other ODBDC driver than the one provided, trying...\n")
                for driver in pyodbc.drivers():
                    print(f"Trying driver: {driver}")
                    if "SQL Server" in driver:
                        if "?" not in self.connection_string:
                            self.connection_string += f"?driver={driver}"
                        else:
                            self.connection_string += f"&driver={driver}"
                        break

                if "driver" not in self.connection_string:
                    raise pyodbc.DatabaseError(
                        "No working ODBC drivers found for SQL Server/pyodbc. "
                        "Please make sure at least one is installed"
                    )

            return

        self.connection_string = "mssql://"
        f"{settings['username']}:{settings['password']}"
        f"@{settings['host']}:{settings['port']}/{settings['database']}"


@register_source("sqlserver", SqlserverConfiguration, SqlserverConnectionSettings)
class SqlserverSource(SourceInterface):
    ConnectionSettingsClass = SqlserverConnectionSettings
    SourceConfigClass = SqlserverConfiguration

    """ A source for SqlServer data """

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
                    ["[" + col.name + "]" for col in self.source_configuration.columns]
                )

                query = f"SELECT {cols} FROM [{self.source_configuration.table}]"

        if returnData:
            pdd = pd.read_sql_query(
                sql=query, con=self.connection_settings.connection_string
            )
            
            return pl.from_pandas(pdd)
        with sqla.create_engine(
            self.connection_settings.connection_string
        ).connect() as con:
            if isinstance(query, str):
                query = sqla.text(query)

            con.execute(query)
            con.commit()

    def _insert(
        self, data: pl.DataFrame, table: str = None, connection_string: str = None, tempDB: bool = False
    ):
        # Check for binary tables
        for col in self.source_configuration.columns:
            if col.type == pl.Binary:
                data = data.with_columns(pl.col(col.name).apply(lambda x: bytes(x), return_dtype=pl.Binary))
                # data[col.name] = data[col.name].apply(lambda x: bytes(x), return_dtype=pl.Binary)        

        if table is None:
            table = self.source_configuration.table

        if connection_string is None:
            connection_string = self.connection_settings.connection_string
        
        pand_df = data.to_pandas()
        try:
            engine = sqla.create_engine(connection_string)
            pand_df.to_sql(table, if_exists="append" if not tempDB else "replace", index=False, con=engine)
        except ModuleNotFoundError:
            data.write_database(
                table,
                self.connection_settings.connection_string.replace(
                    "mysql://", "mysql+pymysql://"
                ),
                if_exists="append" if not tempDB else "replace",
            )

        return len(data)

    def insert(self, data: pl.DataFrame):
        if len(data) == 0:
            return 0
        
        return self._insert(data)

    def update(self, data: pl.DataFrame):
        if len(data) == 0:
            return 0
        
        tempDB = "##" + self.source_configuration.table + "_udtemp"

        # Insert data into temporary table
        self._insert(data, table=tempDB, tempDB=True)

        field_spec = ", ".join(
            (
                f"TDEST.[{fld.name}] = TTEMP.[{fld.name}]"
                for fld in self.source_configuration.columns
                if not fld.skip_update and not fld.unique
            )
        )

        on_clause = " AND ".join(
            (
                f"TDEST.[{fld.name}] = TTEMP.[{fld.name}]"
                for fld in self.source_configuration.columns
                if fld.unique
            )
        )

        query = f"""
            UPDATE TDEST
            SET {field_spec}
            FROM {self.source_configuration.table} AS TDEST
            INNER JOIN {tempDB} AS TTEMP ON {on_clause}
        """

        self._query(customQuery=query, returnData=False)
        self._query(customQuery="DROP TABLE " + tempDB, returnData=False)

        return len(data)

    def delete(self, data: pl.DataFrame):
        if len(data) == 0:
            return 0
        
        tempDB = "##" + self.source_configuration.table + "_ddtemp"
        self._insert(data, table=tempDB, tempDB=True)

        where_clause = " AND ".join(
            (
                f"[{fld.name}] IN (SELECT [{fld.name}] FROM {tempDB}) "
                for fld in self.source_configuration.columns
                if fld.unique
            )
        )

        where_clause = " AND ".join(
            (
                f"TTEMP.[{fld.name}] = TDEST.[{fld.name}]"
                for fld in self.source_configuration.columns
                if fld.unique
            )
        )

        if self.source_configuration.soft_delete:
            if self.source_configuration.deleted_field is None or self.source_configuration.deleted_value is None:
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

        return len(data)
