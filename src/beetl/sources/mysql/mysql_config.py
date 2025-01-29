from typing import Annotated, Optional

from pydantic import ConfigDict, Field, model_validator

from ..interface import SourceConfig, SourceConfigArguments, SourceConnectionArguments


class MysqlConnectionArguments(SourceConnectionArguments):
    connection_string: Annotated[Optional[str], Field(default=None)]
    username: Annotated[Optional[str], Field(default=None)]
    password: Annotated[Optional[str], Field(default=None)]
    host: Annotated[Optional[str], Field(default=None)]
    port: Annotated[Optional[str], Field(default=None)]
    database: Annotated[Optional[str], Field(default=None)]

    @model_validator(mode="after")
    def validate_connection_string_or_components(
        cls, instance: "MysqlConfigArguments.Connection"
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
        if arguments.connection.connection_string:
            connection_string = arguments.connection.connection_string
        if not arguments.connection.connection_string:
            connection_string = "mysql+pymysql://"
            f"{arguments.connection.username}:{arguments.connection.password}"
            f"@{arguments.connection.host}:{arguments.connection.port}/{arguments.connection.database}"

        self.connection_string = connection_string
