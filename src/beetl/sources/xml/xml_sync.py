from typing import Annotated, Literal, Optional

from pydantic import Field, model_validator

from ...errors import (
    ConfigValidationError,
    ConfigValueError,
    RequiredDestinationFieldError,
)
from ..interface import SourceSync, SourceSyncArguments

polar_to_xml_type_map = {
    "Float32": "Float32",
    "Float64": "Float64",
    "Int8": "Int8",
    "Int16": "Int16",
    "Int32": "Int32",
    "Int64": "Int64",
    "Int128": "Int128",
    "UInt8": "UInt8",
    "UInt16": "UInt16",
    "UInt32": "UInt32",
    "UInt64": "UInt64",
    "String": "str",
    "str": "str",
    "Utf8": "str",
    "Boolean": "bool",
}


class XmlSyncArguments(SourceSyncArguments):
    xpath: Annotated[str, Field(default="./*")]
    namespaces: Annotated[Optional[dict[str, str]], Field(default=None)]
    unique_columns: Annotated[tuple[str, ...], Field(default=tuple([]))]
    root_name: Annotated[str, Field(default="root")]
    row_name: Annotated[str, Field(default="row")]
    types: Annotated[Optional[dict[str, str]], Field(default=None)]
    xsl: Annotated[Optional[str], Field(default=None)]
    type: Annotated[Literal["Xml"], Field(default="Xml")] = "Xml"

    @model_validator(mode="after")
    def validate_types(cls, arguments: "XmlSyncArguments"):
        errors: list[Exception] = []
        if arguments.types is not None:
            for key, value in arguments.types.items():
                if value not in polar_to_xml_type_map:
                    errors.append(
                        ConfigValueError(
                            f"types.{key}",
                            f"Type '{value}' is not supported. Supported types are {polar_to_xml_type_map.keys()}.",
                            arguments.location,
                        )
                    )
        return arguments

    @model_validator(mode="after")
    def validate_as_destination(cls, instance: "XmlSyncArguments"):
        if instance.direction == "source":
            return instance
        errors: list[Exception] = []
        if not instance.unique_columns:
            errors.append(
                RequiredDestinationFieldError("unique_columns", instance.location)
            )

        if errors:
            raise ConfigValidationError(errors)

        return instance


class XmlSync(SourceSync):
    """The configuration class used for XML file sources"""

    xpath: str = ""
    namespaces: Optional[dict[str, str]] = None
    unique_columns: tuple[str] = ()
    root_name: str = ""
    row_name: str = ""
    types: dict = None
    xsl: Optional[str] = None
    parser: Literal["etree", "lxml"] = "etree"

    def __init__(self, arguments: XmlSyncArguments):
        super().__init__(arguments)
        self.xpath = arguments.xpath
        self.unique_columns = arguments.unique_columns
        self.root_name = arguments.root_name
        self.row_name = arguments.row_name
        self.namespaces = arguments.namespaces

        if arguments.xsl is not None:
            self.xsl = arguments.xsl
            self.parser = "lxml"

        if arguments.types is not None:
            self.types = {}
            for key, value in arguments.types.items():
                self.types[key] = polar_to_xml_type_map[value]
