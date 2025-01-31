from typing import Annotated

from pydantic import Field, model_validator

from ...validation import ValidationBaseModel


class SourceConnectionArguments(ValidationBaseModel):
    pass


class SourceConfigArguments(ValidationBaseModel):
    """Class representation of the source connection settings in the beetl config. Used to validate the source configuration settings using pydantic."""

    name: Annotated[str, Field(min_length=1)]
    type: Annotated[str, Field(min_length=1)]
    connection: SourceConnectionArguments

    @model_validator(mode="before")
    def propagate_nested_location(cls, values: dict):
        cls.propagate_location("connection", values)
        return values


class SourceConfig:
    """The connection configuration class used for data sources, abstract"""

    def __init__(cls, arguments: SourceConfigArguments):
        pass
