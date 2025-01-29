from typing import Annotated, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ...validation import ValidationBaseModel
from ..interface import SourceConfig, SourceConfigArguments, SourceConnectionArguments


class MongodbSettingsArguments(ValidationBaseModel):
    connection_string: Annotated[Optional[str], Field(min_length=1, default=None)]
    host: Annotated[Optional[str], Field(min_length=1, default=None)]
    port: Annotated[Optional[str], Field(min_length=1, default=None)]
    username: Annotated[Optional[str], Field(min_length=1, default=None)]
    password: Annotated[Optional[str], Field(min_length=1, default=None)]
    database: Annotated[str, Field(min_length=1)]

    @model_validator(mode="after")
    def validate_connection_string_or_components(
        cls, instance: "MongodbSettingsArguments"
    ):
        connection_string_is_not_present = not instance.connection_string
        connection_string_components = ["host", "port", "username", "password"]
        if connection_string_is_not_present:
            dict = instance.model_dump()
            for component in connection_string_components:
                if not dict.get(component, None):
                    raise ValueError(
                        f"'{component}' is missing. {connection_string_components} are required if 'connection_string' is not provided"
                    )

        return instance


class MongodbConnectionArguments(SourceConnectionArguments):
    settings: MongodbSettingsArguments

    @model_validator(mode="before")
    def propagate_nested_location(cls, values: dict):
        cls.propagate_location("settings", values)
        return values


class MongodbConfigArguments(SourceConfigArguments):

    type: Annotated[Literal["Mongodb"], Field(default="Mongodb")] = "Mongodb"
    connection: MongodbConnectionArguments


class MongodbConfig(SourceConfig):
    """The connection configuration class used for MongoDB sources"""

    connection_string: str
    query: Optional[str] = None
    database: str

    def __init__(self, arguments: MongodbConfigArguments):
        super().__init__(arguments)

        connection_string: str
        if arguments.connection.settings.connection_string:
            connection_string = arguments.connection.settings.connection_string
        else:
            connection_string = f"mongodb://{arguments.connection.settings.username}:{arguments.connection.settings.password}@{arguments.connection.settings.host}:{arguments.connection.settings.port}/"

        self.connection_string = connection_string
        self.database = arguments.connection.settings.database
