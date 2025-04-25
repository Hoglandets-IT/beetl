from typing import Annotated, Any, Literal

from polars import DataFrame
from pydantic import Field

from ..interface import SourceConfig, SourceConfigArguments, SourceConnectionArguments


class FakerConnectionArguments(SourceConnectionArguments):
    faker: list[dict[str, Any]]


class FakerConfigArguments(SourceConfigArguments):

    type: Annotated[Literal["Faker"], Field(default="Faker")] = "Faker"
    connection: FakerConnectionArguments


class FakerConfig(SourceConfig):
    """The connection configuration class used for faker sources"""

    data: DataFrame

    def __init__(self, arguments: FakerConfigArguments):
        super().__init__(arguments)
        self.data = DataFrame(arguments.connection.faker or [])
