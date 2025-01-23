from typing import Annotated, Any, List, Literal
from pydantic import BaseModel, ConfigDict, Field
from polars import DataFrame
from .interface import (
    register_source,
    SourceInterface,
    SourceSync,
    SourceConfig,
    SourceConfigArguments
)


class StaticConfigArguments(SourceConfigArguments):
    class ConnectionArguments(BaseModel):
        static: List[dict[str, Any]]

    type: Literal["Static"] = "Static"
    connection: ConnectionArguments


class StaticConfig(SourceConfig):
    """The connection configuration class used for static sources"""
    data: DataFrame

    def __init__(self, arguments: StaticConfigArguments):
        super().__init__(arguments)
        self.data = DataFrame(arguments.connection.static or [])


@ register_source("Static")
class StaticSource(SourceInterface):
    ConfigArgumentsClass = StaticConfigArguments
    ConfigClass = StaticConfig

    """ A source for static data """

    def _configure(self):
        pass

    def _connect(self):
        pass

    def _disconnect(self):
        pass

    def _query(self, params=None) -> DataFrame:
        return self.connection_settings.data

    def insert(self, data: DataFrame):
        print("Inserting data into static source...")
        print(data)
        return len(data)

    def update(self, data: DataFrame):
        print("Updating data in static source...")
        print(data)
        return len(data)

    def delete(self, data: DataFrame):
        print("Deleting data from static source")
        print(data)
        return len(data)
