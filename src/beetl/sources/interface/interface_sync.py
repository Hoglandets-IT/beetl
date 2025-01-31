from typing import Annotated, Literal

from pydantic import Field

from ...validation import ValidationBaseModel


class SourceSyncArguments(ValidationBaseModel):
    """Class representation of the source configuration settings in the beetl config. Used to validate the source configuration settings using pydantic."""

    name: Annotated[str, Field(min_length=1)]
    type: Annotated[Literal["Interface"], Field(default="Interface")] = "Interface"
    direction: Annotated[Literal["source", "destination"], Field(min_length=1)]


class SourceSync:
    """The configuration class used for data sources, abstract"""

    def __init__(cls, arguments: SourceSyncArguments):
        pass
