# Docstrings not necessary for schemas since you never use them directly
# pylint: disable=missing-docstring
from typing import Annotated, Any, Literal, Optional, Union

from pydantic import Field

from .interface import TransformerConfigBase, TransformerSchemaBase


class Mapping(TransformerConfigBase):
    # Needs an alias since from is a protected keyword
    from_: Annotated[str, Field(min_length=1, alias="from")]
    to: Annotated[str, Field(min_length=1)]


class FramesTransformerSchema:
    class Filter(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            filter: Annotated[dict[str, Any], Field(min_items=1)]
            reverse: Annotated[Optional[bool], Field(default=False)]

        transformer: Literal["frames.filter"]
        config: Config

    class Conditional(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            conditionField: Annotated[str, Field(min_length=1)]
            ifTrue: str
            ifFalse: str
            targetField: Annotated[str, Field(default="")]

        transformer: Literal["frames.conditional"]
        config: Config

    class RenameColumns(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            columns: Annotated[
                Union[list[Mapping], dict[str, str]], Field(min_length=1)
            ]

        transformer: Literal["frames.rename_columns"]
        config: Config

    class CopyColumns(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            columns: Annotated[list[Mapping], Field(min_length=1)]

        transformer: Literal["frames.copy_columns"]
        config: Config

    class DropColumns(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            columns: Annotated[list[str], Field(min_length=1)]

        transformer: Literal["frames.drop_columns"]
        config: Config

    class ProjectColumns(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            columns: Annotated[list[str], Field(min_length=1)]

        transformer: Literal["frames.project_columns"]
        config: Config

    class Distinct(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            columns: Annotated[list[str], Field(min_length=1)]

        transformer: Literal["frames.distinct"]
        config: Config

    class ExtractNestedRows(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            iterField: Annotated[str, Field(default="")]
            fieldMap: Annotated[dict[str, str], Field(default={})]
            colMap: Annotated[dict[str, str], Field(default={})]

        transformer: Literal["frames.extract_nested_rows"]
        config: Config


FramesTransformerSchemas = Union[
    FramesTransformerSchema.Filter,
    FramesTransformerSchema.Conditional,
    FramesTransformerSchema.RenameColumns,
    FramesTransformerSchema.CopyColumns,
    FramesTransformerSchema.DropColumns,
    FramesTransformerSchema.ProjectColumns,
    FramesTransformerSchema.Distinct,
    FramesTransformerSchema.ExtractNestedRows,
]
