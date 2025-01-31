from typing import Annotated, Optional

from pydantic import Field, model_validator

from ...validation import ValidationBaseModel
from ..interface import SourceConfig, SourceConfigArguments, SourceConnectionArguments


class MysqlSettingsArguments(ValidationBaseModel):
    connection_string: Annotated[Optional[str], Field(default=None)]
    username: Annotated[Optional[str], Field(default=None)]
    password: Annotated[Optional[str], Field(default=None)]
    host: Annotated[Optional[str], Field(default=None)]
    port: Annotated[Optional[str], Field(default=None)]
    database: Annotated[Optional[str], Field(default=None)]

    @model_validator(mode="after")
    def validate_connection_string_or_components(
        cls, instance: "MysqlSettingsArguments"
    ):
        connection_string_is_not_present = not instance.connection_string
        connection_string_components = [
            "host",
            "port",
            "username",
            "password",
            "database",
        ]
        if connection_string_is_not_present:
            dict = instance.model_dump()
            for component in connection_string_components:
                if not dict.get(component, None):
                    raise ValueError(
                        f"'{component}' is missing. {connection_string_components} are required if 'connection_string' is not provided"
                    )
        return instance


class MysqlConnectionArguments(SourceConnectionArguments):
    settings: MysqlSettingsArguments

    @model_validator(mode="before")
    def propagate_nested_location(cls, values: dict):
        cls.propagate_location("settings", values)
        return values


class MysqlConfigArguments(SourceConfigArguments):
    type: Annotated[str, Field(default="Mysql")] = "Mysql"
    connection: MysqlConnectionArguments


class MysqlConfig(SourceConfig):
    """The connection configuration class used for MySQL sources"""

    connection_string: str
    query: str = None
    table: str = None

    def __init__(self, arguments: MysqlConfigArguments):
        super().__init__(arguments)

        connection_string: str
        if arguments.connection.settings.connection_string:
            connection_string = arguments.connection.settings.connection_string
        if not arguments.connection.settings.connection_string:
            connection_string = "mysql+pymysql://"
            f"{arguments.connection.settings.username}:{arguments.connection.settings.password}"
            f"@{arguments.connection.settings.host}:{arguments.connection.settings.port}/{arguments.connection.settings.database}"

        self.connection_string = connection_string
