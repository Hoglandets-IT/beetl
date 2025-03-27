from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


class SourceDiffConfigArguments(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SourceDiffArguments(BaseModel):
    """Class representation of the diff configuration settings present in a beetl sync. Used to validate the diff configuration settings using pydantic.

    How to use:
    When deriving from this class you need to override the literal of the type field with the registered source name.

    You can then choose to override the default config field if your source requires additional configuration settings.
    """

    model_config = ConfigDict(extra="forbid")

    type: Literal["Interface"]
    name: Annotated[str, Field(min_length=1)]
    config: SourceDiffConfigArguments


class SourceDiff:
    def __init__(self, arguments: SourceDiffConfigArguments):
        pass
