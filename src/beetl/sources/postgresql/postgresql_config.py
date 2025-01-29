from typing import Annotated, Literal, Optional

from pydantic import Field, model_validator

from ...errors import ConfigValidationError, ConfigValueError
from ...validation import ValidationBaseModel
from ..interface import SourceConfig, SourceConfigArguments, SourceConnectionArguments


class PostgresSettingsArguments(ValidationBaseModel):
    connection_string: Annotated[Optional[str], Field(default=None)]
    username: Annotated[Optional[str], Field(min_length=1, default=None)]
    password: Annotated[Optional[str], Field(min_length=1, default=None)]
    host: Annotated[Optional[str], Field(min_length=1, default=None)]
    port: Annotated[Optional[str], Field(min_length=1, default=None)]
    database: Annotated[Optional[str], Field(min_length=1, default=None)]

    @model_validator(mode="after")
    def validate_connection_string(
        cls, instance: "PostgresConfigArguments.ConnectionArguments"
    ):
        if instance.connection_string:
            return instance

        errors = []
        fields = ["username", "password", "host", "port", "database"]
        for field in fields:
            if getattr(instance, field) is None:
                errors.append(
                    ConfigValueError(
                        field,
                        f"'{field}' is required when 'connection_string' is not provided",
                        instance.location,
                    )
                )

        if errors:
            raise ConfigValidationError(errors)
        return instance


class PostgresConnectionArguments(SourceConnectionArguments):
    settings: PostgresSettingsArguments

    @model_validator(mode="before")
    def propagate_nested_location(cls, values: "PostgresConnectionArguments"):
        cls.propagate_location("settings", values)
        return values


class PostgresConfigArguments(SourceConfigArguments):
    type: Annotated[Literal["Postgresql"], Field(default="Postgresql")] = "Postgresql"
    connection: PostgresConnectionArguments


class PostgresConfig(SourceConfig):
    """The connection configuration class used for Postgresql sources"""

    connection_string: str

    def __init__(self, arguments: PostgresConfigArguments):
        super().__init__(arguments)
        self.connection_string = arguments.connection.settings.connection_string

        if not self.connection_string:
            self.connection_string = f"postgresql://{arguments.connection.settings.username}:{arguments.connection.settings.password}@{arguments.connection.settings.host}:{arguments.connection.settings.port}/{arguments.connection.settings.database}"
