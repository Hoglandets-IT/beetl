from typing import Annotated, Literal, Optional

from pydantic import Field, model_validator

from ...errors import (
    ConfigValidationError,
    RequiredDestinationFieldError,
    RequiredSourceFieldError,
)
from ..interface import SourceSync, SourceSyncArguments


class PostgresSyncArguments(SourceSyncArguments):
    table: Annotated[Optional[str], Field(default=None)]
    query: Annotated[Optional[str], Field(default=None)]
    uniqueColumns: Annotated[list[str], Field(default=[])]
    skipColumns: Annotated[list[str], Field(default=[])]
    type: Annotated[Literal["Postgresql"], Field(defauls="Postgresql")] = "Postgresql"

    @model_validator(mode="after")
    def validate_as_source(cls, instance: "PostgresSyncArguments"):
        if instance.direction == "destination":
            return instance
        errors = []
        if not instance.table and not instance.query:
            errors.append(RequiredSourceFieldError("table|query", instance.location))

        if errors:
            raise ConfigValidationError(errors)
        return instance

    @model_validator(mode="after")
    def validate_as_destination(cls, instance: "PostgresSyncArguments"):
        if instance.direction == "source":
            return instance
        errors = []
        if not instance.table:
            errors.append(RequiredDestinationFieldError("table", instance.location))
        if not instance.uniqueColumns:
            errors.append(
                RequiredDestinationFieldError("uniqueColumns", instance.location)
            )

        if errors:
            raise ConfigValidationError(errors)

        return instance


class PostgresSync(SourceSync):
    """The configuration class used for Postgresql sources"""

    unique_columns: list[str] = None
    skip_columns: list[str] = None
    table: str = None
    query: str = None

    def __init__(self, arguments: PostgresSyncArguments):
        super().__init__(arguments)

        self.table = arguments.table
        self.query = arguments.query
        self.unique_columns = arguments.uniqueColumns
        self.skip_columns = arguments.skipColumns
