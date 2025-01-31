from typing import Any, Literal

from polars import DataFrame
from pydantic import BaseModel

from ..interface import SourceConfig, SourceConfigArguments


class StaticConfigArguments(SourceConfigArguments):
    class ConnectionArguments(BaseModel):
        static: list[dict[str, Any]]

    type: Literal["Static"] = "Static"
    connection: ConnectionArguments


class StaticConfig(SourceConfig):
    """The connection configuration class used for static sources"""

    data: DataFrame

    def __init__(self, arguments: StaticConfigArguments):
        super().__init__(arguments)
        self.data = DataFrame(arguments.connection.static or [])
