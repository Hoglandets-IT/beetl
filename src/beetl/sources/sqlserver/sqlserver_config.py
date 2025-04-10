from typing import Annotated, Literal, Optional

from pydantic import Field, model_validator
from pyodbc import DatabaseError, drivers

from ...errors import (
    ConfigValidationError,
    ConfigValueError,
    InvalidDependencyError,
    MissingDependencyError,
)
from ...validation import ValidationBaseModel
from ..interface import SourceConfig, SourceConfigArguments, SourceConnectionArguments


class SqlserverSettingsArguments(ValidationBaseModel):
    connection_string: Annotated[
        Optional[str],
        Field(
            default=None,
            description="Required if individual connection settings are not provided",
        ),
    ]
    username: Annotated[
        Optional[str],
        Field(
            default=None, description="Required if connection string is not provided"
        ),
    ]
    password: Annotated[
        Optional[str],
        Field(
            default=None, description="Required if connection string is not provided"
        ),
    ]
    host: Annotated[
        Optional[str],
        Field(
            default=None, description="Required if connection string is not provided"
        ),
    ]
    port: Annotated[
        Optional[str],
        Field(
            default=None, description="Required if connection string is not provided"
        ),
    ]
    database: Annotated[
        Optional[str],
        Field(
            default=None, description="Required if connection string is not provided"
        ),
    ]

    @model_validator(mode="after")
    def validate_connection_settings(cls, instance: "SqlserverConnectionArguments"):
        errors = []

        sql_drivers = [driver for driver in drivers() if "SQL Server" in driver]
        if len(sql_drivers) == 0:
            errors.append(
                MissingDependencyError(
                    "connection_string",
                    "No ODBC drivers were found for SQL Server/pyodbc. Please install the Microsoft ODBC Driver for SQL Server",
                    instance.location,
                )
            )

        if len(sql_drivers) == 1 and sql_drivers[0] == "SQL Server":
            errors.append(
                InvalidDependencyError(
                    "connection_string",
                    "Your system has an old version of the ODBC driver for SQL Server."
                    "Please install a more recent version of the driver (Microsoft ODBC Driver 17 for SQL Server or above)",
                    instance.location,
                )
            )

        if instance.connection_string:
            if (
                "driver=SQL+Server" in instance.connection_string
                or "driver=SQL Server" in instance.connection_string
            ):
                errstring = "You need to download a more recent version of the driver (Microsoft ODBC Driver 17 for SQL Server or above)"
                found = [driver for driver in drivers() if driver.startswith("ODBC")]
                if len(found) > 0:
                    errstring = f"You have the following ODBC drivers installed, please use one of them or download a more recent version: {found.join(', ')}"

                errors.append(
                    InvalidDependencyError(
                        "connection_string",
                        "The connection string contains the driver 'SQL Server' or 'SQL+Server'. "
                        + errstring,
                        instance.location,
                    )
                )

            if len(sql_drivers) == 1 and sql_drivers[0] == "SQL Server":
                errors.append(ConfigValueError("connection_string"))

            return instance

        required_fields = ["username", "password", "host", "port", "database"]
        for field in required_fields:
            if not getattr(instance, field):
                errors.append(
                    ConfigValueError(
                        field,
                        f"Field '{field}' is required when 'connection_string' is not provided",
                        instance.location,
                    )
                )

        if errors:
            raise ConfigValidationError(errors)

        return instance


class SqlserverConnectionArguments(SourceConnectionArguments):
    settings: SqlserverSettingsArguments

    @model_validator(mode="before")
    def propagate_nested_location(cls, arguments: "SqlserverConnectionArguments"):
        cls.propagate_location("settings", arguments)
        return arguments


class SqlserverConfigArguments(SourceConfigArguments):
    connection: SqlserverConnectionArguments
    type: Annotated[Literal["Sqlserver"], Field(default="Sqlserver")] = "Sqlserver"


class SqlserverConfig(SourceConfig):
    """The connection configuration class used for SQLServer sources"""

    connection_string: str

    def append_sqlserver_driver(self, connection_string: str):
        if "driver" in connection_string:
            return connection_string

        sql_drivers = [driver for driver in drivers() if "SQL Server" in driver]
        no_drivers_available = len(sql_drivers) == 0
        if no_drivers_available:
            raise DatabaseError(
                "No ODBC drivers found for SQL Server/pyodbc. "
                "Please make sure at least one is installed"
            )

        print(
            f"\nFound driver '{sql_drivers[0]}', beetl is automatically appending it to the connection string for you. If you want to use a different driver, please add it manually."
        )
        join_char = "&" if "?" in connection_string else "?"
        return f"{connection_string}{join_char}driver={sql_drivers[0]}"

    def __init__(self, arguments: SqlserverConfigArguments):
        super().__init__(arguments)

        if arguments.connection.settings.connection_string:
            self.connection_string = self.append_sqlserver_driver(
                arguments.connection.settings.connection_string
            )
            return

        composed_connection_string = "mssql://"
        f"{arguments.connection.settings.username}:{arguments.connection.settings.password}"
        f"@{arguments.connection.settings.host}:{arguments.connection.settings.port}/{arguments.connection.settings.database}"

        self.connection_string = self.append_sqlserver_driver(
            composed_connection_string
        )
