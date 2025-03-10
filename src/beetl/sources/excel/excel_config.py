from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from ..interface import SourceConfig, SourceConfigArguments, SourceConnectionArguments


class ExcelConnectionArguments(SourceConnectionArguments):
    path: Annotated[str, Field(min_length=1)]
    
class ExcelConfigArguments(SourceConfigArguments):

    type: Annotated[Literal["Excel"], Field(default="Excel")] = "Excel"
    connection: ExcelConnectionArguments


class ExcelConfig(SourceConfig):
    """The connection configuration class used for static sources"""

    path: str

    def __init__(self, arguments: ExcelConfigArguments):
        super().__init__(arguments)
        self.path = arguments.connection.path
