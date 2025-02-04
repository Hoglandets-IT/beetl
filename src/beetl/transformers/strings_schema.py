from typing import Annotated, Literal, Optional, Union

from pydantic import Field, model_validator

from .interface import TransformerConfigBase, TransformerSchemaBase


class StringTransformerSchema:
    class StaticField(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            field: Annotated[str, Field(min_length=1)]
            value: str

        transformer: Literal["strings.staticfield"]
        config: "StringTransformerSchema.StaticField.Config"

    class SetDefaults(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            defaultValue: str

        transformer: Literal["strings.set_default"]
        config: "StringTransformerSchema.SetDefaults.Config"

    class Strip(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            stripChars: str
            outField: Annotated[Optional[str], Field(default=None)]

        transformer: Literal["strings.strip"]
        config: "StringTransformerSchema.Strip.Config"

    class Lowercase(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[Optional[str], Field(default=None)]
            inOutMap: Annotated[dict[str, str], Field(default={})]

        transformer: Literal["strings.lowercase"]
        config: "StringTransformerSchema.Lowercase.Config"

    class Uppercase(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            outField: Annotated[Optional[str], Field(default=None)]
            inOutMap: Annotated[dict[str, str], Field(default={})]
            inField: Annotated[str, Field(default="")]

            @model_validator(mode="wrap")
            def model_validator(cls, values: dict, handler):
                model: "StringTransformerSchema.Uppercase.Config" = handler(values)

                in_out_map_missing_or_empty: bool = (
                    not model.inOutMap or len(model.inOutMap) == 0
                )
                if in_out_map_missing_or_empty:
                    if model.inField is None:
                        raise ValueError(
                            "inField is required when inOutMap is missing or empty"
                        )
                    if len(model.inField) == 0:
                        raise ValueError("inField must have a length greater than 0")
                return model

        transformer: Literal["strings.uppercase"]
        config: "StringTransformerSchema.Uppercase.Config"

    class MatchContains(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[Optional[str], Field(default=None)]
            match: str

        transformer: Literal["strings.match_contains"]
        config: "StringTransformerSchema.MatchContains.Config"

    class Join(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inFields: Annotated[list[str], Field(min_length=1)]
            outField: Annotated[str, Field(min_length=1)]
            separator: Annotated[str, Field(default="")]

        transformer: Literal["strings.join"]
        config: "StringTransformerSchema.Join.Config"

    class JoinListfield(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[str, Field(min_length=1)]
            separator: Annotated[str, Field(default="")]

        transformer: Literal["strings.join_listfield"]
        config: "StringTransformerSchema.JoinListfield.Config"

    class Split(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outFields: Annotated[list[str], Field(min_length=1)]
            separator: Annotated[str, Field(default="")]

        transformer: Literal["strings.split"]
        config: "StringTransformerSchema.Split.Config"

    class SplitIntoListfield(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[Optional[str], Field(default=None)]
            separator: Annotated[str, Field(default="")]

        transformer: Literal["strings.split_into_listfield"]
        config: "StringTransformerSchema.SplitIntoListfield.Config"

    class Quote(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[Optional[str], Field(default=None)]
            quote: Annotated[str, Field(default="'")]

        transformer: Literal["strings.quote"]
        config: "StringTransformerSchema.Quote.Config"

    class Replace(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[Optional[str], Field(default=None)]
            search: str
            replace: str

        transformer: Literal["strings.replace"]
        config: "StringTransformerSchema.Replace.Config"

    class ReplaceAll(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[Optional[str], Field(default=None)]
            search: str
            replace: str

        transformer: Literal["strings.replace_all"]
        config: "StringTransformerSchema.ReplaceAll.Config"

    class Substring(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[Optional[str], Field(default=None)]
            start: Annotated[int, Field(default=0)]
            length: Annotated[Optional[int], Field(default=None)]

        transformer: Literal["strings.substring"]
        config: "StringTransformerSchema.Substring.Config"

    class AddPrefix(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[Optional[str], Field(default=None)]
            prefix: Annotated[str, Field(min_length=1)]

        transformer: Literal["strings.add_prefix"]
        config: "StringTransformerSchema.AddPrefix.Config"

    class Cast(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[str, Field(default="")]

        transformer: Literal["strings.cast"]
        config: "StringTransformerSchema.Cast.Config"

    class Hash(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[str, Field(default="")]

        transformer: Literal["strings.hash"]
        config: "StringTransformerSchema.Hash.Config"

    class ToObjectId(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]

        transformer: Literal["strings.to_object_id"]
        config: "StringTransformerSchema.ToObjectId.Config"


StringTransformerSchemas = Union[
    StringTransformerSchema.StaticField,
    StringTransformerSchema.SetDefaults,
    StringTransformerSchema.Strip,
    StringTransformerSchema.Lowercase,
    StringTransformerSchema.Uppercase,
    StringTransformerSchema.MatchContains,
    StringTransformerSchema.Join,
    StringTransformerSchema.JoinListfield,
    StringTransformerSchema.Split,
    StringTransformerSchema.SplitIntoListfield,
    StringTransformerSchema.Quote,
    StringTransformerSchema.Replace,
    StringTransformerSchema.ReplaceAll,
    StringTransformerSchema.Substring,
    StringTransformerSchema.AddPrefix,
    StringTransformerSchema.Cast,
    StringTransformerSchema.Hash,
    StringTransformerSchema.ToObjectId,
]
