from typing import Annotated, Literal, Optional

from pydantic import Field, model_validator

from ...errors import (
    ConfigValidationError,
    ConfigValueError,
    ForbiddenSourceFieldError,
    RequiredDestinationFieldError,
)
from ..interface import SourceSync, SourceSyncArguments


class SqlserverSyncArguments(SourceSyncArguments):
    type: Annotated[Literal["Sqlserver"], Field(defauls="Sqlserver")] = "Sqlserver"
    table: Annotated[
        Optional[str],
        Field(
            default=None,
            description="Sql server table name. Required when used as a destination and when query isn't provided.",
        ),
    ]
    query: Annotated[
        Optional[str],
        Field(
            default=None,
            description="SQL query. Defaults to all fields in the specified 'table'.",
        ),
    ]
    soft_delete: Annotated[
        Optional[bool],
        Field(
            default=None,
            description="Enables soft delete. Only allowed when used as a destination.",
        ),
    ]
    deleted_field: Annotated[
        Optional[str],
        Field(
            default=None,
            description="Field used for soft delete. Only allowed when used as a destination.",
        ),
    ]
    deleted_value: Annotated[
        Optional[str],
        Field(
            default=None,
            description="Value used for soft delete. Only allowed when used as a destination.",
        ),
    ]
    uniqueColumns: Annotated[
        list[str],
        Field(
            default=[],
            description="Defines what columns to use as unique identifiers. Only allowed when used as a destination.",
        ),
    ]
    skipColumns: Annotated[
        list[str],
        Field(
            default=[],
            description="Defines what columns to skip when inserting and updating. Only allowed when used as a destination.",
        ),
    ]
    replace_empty_strings: Annotated[
        bool,
        Field(
            default=False,
            description="If True, empty strings ('') in text columns will be replaced with None.",
        ),
    ]

    @model_validator(mode="after")
    def validate_as_source(cls, instance: "SqlserverSyncArguments"):
        if instance.direction == "destination":
            return instance

        errors = []

        if not instance.query:
            if not instance.table:
                errors.append(
                    ConfigValueError(
                        "table",
                        "Table name is required when query isn't provided",
                        instance.location,
                    )
                )

        forbidden_fields = [
            "deleted_field",
            "deleted_value",
            "soft_delete",
            "uniqueColumns",
            "skipColumns",
        ]
        for field in forbidden_fields:
            if getattr(instance, field):
                errors.append(ForbiddenSourceFieldError(field, instance.location))

        if errors:
            raise ConfigValidationError(errors)

        return instance

    @model_validator(mode="after")
    def validate_as_destination(cls, instance: "SqlserverSyncArguments"):
        if instance.direction == "source":
            return instance

        errors = []
        required_fields = ["uniqueColumns", "table"]
        for field in required_fields:
            if not getattr(instance, field):
                errors.append(
                    RequiredDestinationFieldError(
                        field,
                        instance.location,
                    )
                )

        if instance.soft_delete:
            required_fields = ["deleted_field", "deleted_value"]
            for field in required_fields:
                if not getattr(instance, field):
                    errors.append(
                        ConfigValueError(
                            field,
                            f"Field '{field}' is required when using 'soft_delete' is set to True",
                            instance.location,
                        )
                    )

        if errors:
            raise ConfigValidationError(errors)

        return instance


class SqlserverSync(SourceSync):
    """The configuration class used for SQLServer sources"""

    table: str = None
    query: str = None
    soft_delete: bool = False
    deleted_field: str = None
    deleted_value: str = "'true'"
    unique_columns: list[str] = None
    skip_columns: list[str] = None
    replace_empty_strings: bool = False

    def __init__(
        self,
        arguments: SqlserverSyncArguments,
    ):
        super().__init__(arguments)
        self.table = arguments.table
        self.query = (
            arguments.query if arguments.query else f"SELECT * FROM {arguments.table}"
        )
        self.soft_delete = arguments.soft_delete
        self.deleted_field = arguments.deleted_field
        self.deleted_value = arguments.deleted_value
        self.unique_columns = arguments.uniqueColumns
        self.skip_columns = arguments.skipColumns
        self.replace_empty_strings = arguments.replace_empty_strings
