from typing import Annotated, Any, Literal
from polars import DataFrame as POLARS_DF
from pydantic import BaseModel, ConfigDict, Field
from .interface import (
    SourceInterface,
    SourceSync,
    SourceConfig,
    SourceConfigArguments,
    register_source,
)


class FakerConfigArguments(SourceConfigArguments):
    class FakerConnectionArguments(BaseModel):
        model_config = ConfigDict(extra='forbid')

        faker: list[dict[str, Any]]

    type: Annotated[Literal["Faker"], Field(default="Faker")] = "Faker"
    connection: FakerConnectionArguments


class FakerConfig(SourceConfig):
    """The connection configuration class used for faker sources"""
    data: POLARS_DF

    def __init__(self, arguments: FakerConfigArguments):
        super().__init__(arguments)
        self.data = POLARS_DF(arguments.connection.faker or [])


@register_source("Faker")
class FakerSource(SourceInterface):
    ConfigArgumentsClass = FakerConfigArguments
    ConfigClass = FakerConfig

    """ A source for faker data """

    def _configure(self):
        pass

    def _connect(self):
        pass

    def _disconnect(self):
        pass

    def _query(self, params=None) -> POLARS_DF:
        return self.connection_settings.data

    def insert(self, data: POLARS_DF):
        print("Inserting data into static source...")
        print(data)
        return len(data)

    def update(self, data: POLARS_DF):
        print("Updating data in static source...")
        print(data)
        return len(data)

    def delete(self, data: POLARS_DF):
        print("Deleting data from static source")
        print(data)
        return len(data)
