from typing import Annotated, Literal, Optional

from pydantic import Field, model_validator
from pyodbc import DatabaseError, drivers

from ...errors import ConfigValidationError, ConfigValueError
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
        if instance.connection_string:
            return instance

        errors = []
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
