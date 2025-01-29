from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from ..interface import SourceConfig, SourceConfigArguments


class CsvConfigArguments(SourceConfigArguments):
    class CsvConnectionArguments(BaseModel):
        model_config = ConfigDict(extra="forbid")

        path: Annotated[str, Field(min_length=1)]
        encoding: Annotated[str, Field(default="utf-8")]

    type: Annotated[Literal["Csv"], Field(default="Csv")] = "Csv"
    connection: CsvConnectionArguments


class CsvConfig(SourceConfig):
    """The connection configuration class used for static sources"""

    path: str
    encoding: str

    def __init__(self, arguments: CsvConfigArguments):
        super().__init__(arguments)
        self.path = arguments.connection.path
        self.encoding = arguments.connection.encoding
