from typing import Annotated, Literal

from pydantic import Field

from ..interface import SourceConfig, SourceConfigArguments, SourceConnectionArguments


class XmlConnectionArguments(SourceConnectionArguments):
    path: Annotated[str, Field(min_length=1)]
    encoding: Annotated[str, Field(default="utf-8")]


class XmlConfigArguments(SourceConfigArguments):
    connection: XmlConnectionArguments
    type: Annotated[Literal["Xml"], Field(default="Xml")] = "Xml"


class XmlConfig(SourceConfig):
    """The connection configuration class used for XML file sources"""

    path: str
    encoding: str = "utf-8"

    def __init__(self, arguments: XmlConfigArguments):
        super().__init__(arguments)
        self.path = arguments.connection.path
        self.encoding = arguments.connection.encoding
