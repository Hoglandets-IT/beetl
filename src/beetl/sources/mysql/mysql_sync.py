from typing import Annotated, Literal, Optional

from pydantic import ConfigDict, Field, model_validator

from ...errors import ConfigValidationError, RequiredDestinationFieldError
from ..interface import SourceSync, SourceSyncArguments


class MysqlSyncArguments(SourceSyncArguments):
    model_config = ConfigDict(extra="forbid")

    table: Annotated[
        Optional[str],
        Field(
            default=None,
            description="Mandatory if configured as a destination, and if query isn't provided",
        ),
    ]
    query: Annotated[Optional[str], Field(default=None)]
    uniqueColumns: Annotated[list[str], Field(default=[])]
    skipColumns: Annotated[list[str], Field(default=[])]
    type: Annotated[Literal["Mysql"], Field(defauls="Mysql")] = "Mysql"

    @model_validator(mode="after")
    def validate_as_source(cls, instance: "MysqlSyncArguments"):
        if instance.direction == "destination":
            return instance

        if not instance.table and not instance.query:
            raise ValueError("'table' or 'query' is required when used as a source")

        return instance

    @model_validator(mode="after")
    def validate_as_destination(cls, instance: "MysqlSyncArguments"):
        if instance.direction == "source":
            return instance

        errors = []

        if not instance.table and not instance.query:
            errors.append(RequiredDestinationFieldError("table", instance.location))

        if not instance.uniqueColumns:
            errors.append(
                RequiredDestinationFieldError("uniqueColumns", instance.location)
            )

        if errors:
            raise ConfigValidationError(errors)

        return instance


class MysqlSync(SourceSync):
    """The configuration class used for MySQL sources"""

    unique_columns: list[str] = None
    skip_columns: list[str] = None
    table: str = None
    query: str = None

    def __init__(self, arguments: MysqlSyncArguments):
        super().__init__(arguments)

        self.table = arguments.table
        self.query = arguments.query
        self.unique_columns = arguments.uniqueColumns
        self.skip_columns = arguments.skipColumns
