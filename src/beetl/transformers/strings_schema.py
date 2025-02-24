from typing import Annotated, Literal, Optional, Union

from pydantic import Field, model_validator

from .interface import TransformerConfigBase, TransformerSchemaBase


class StringTransformerSchema:
    class StaticField(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            field: Annotated[str, Field(min_length=1)]
            value: str

        transformer: Literal["strings.staticfield"]
        config: Config

    class SetDefaults(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            defaultValue: str

        transformer: Literal["strings.set_default"]
        config: Config

    class Strip(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            stripChars: str
            outField: Annotated[Optional[str], Field(default=None)]

        transformer: Literal["strings.strip"]
        config: Config

    class Lowercase(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[Optional[str], Field(default=None)]
            inOutMap: Annotated[dict[str, str], Field(default={})]

        transformer: Literal["strings.lowercase"]
        config: Config

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
        config: Config

    class MatchContains(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[Optional[str], Field(default=None)]
            match: str

        transformer: Literal["strings.match_contains"]
        config: Config

    class Join(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inFields: Annotated[list[str], Field(min_length=1)]
            outField: Annotated[str, Field(min_length=1)]
            separator: Annotated[str, Field(default="")]

        transformer: Literal["strings.join"]
        config: Config

    class JoinListfield(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[str, Field(min_length=1)]
            separator: Annotated[str, Field(default="")]

        transformer: Literal["strings.join_listfield"]
        config: Config

    class Split(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outFields: Annotated[list[str], Field(min_length=1)]
            separator: Annotated[str, Field(default="")]

        transformer: Literal["strings.split"]
        config: Config

    class SplitIntoListfield(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[Optional[str], Field(default=None)]
            separator: Annotated[str, Field(default="")]

        transformer: Literal["strings.split_into_listfield"]
        config: Config

    class Quote(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[Optional[str], Field(default=None)]
            quote: Annotated[str, Field(default="'")]

        transformer: Literal["strings.quote"]
        config: Config

    class Replace(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[Optional[str], Field(default=None)]
            search: str
            replace: str

        transformer: Literal["strings.replace"]
        config: Config

    class ReplaceAll(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[Optional[str], Field(default=None)]
            search: str
            replace: str

        transformer: Literal["strings.replace_all"]
        config: Config

    class Substring(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[Optional[str], Field(default=None)]
            start: Annotated[int, Field(default=0)]
            length: Annotated[Optional[int], Field(default=None)]

        transformer: Literal["strings.substring"]
        config: Config

    class AddPrefix(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[Optional[str], Field(default=None)]
            prefix: Annotated[str, Field(min_length=1)]

        transformer: Literal["strings.add_prefix"]
        config: Config

    class Cast(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[str, Field(default="")]

        transformer: Literal["strings.cast"]
        config: Config

    class ToObjectId(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]

        transformer: Literal["strings.to_object_id"]
        config: Config

    class Format(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[str, Field(default="")]
            format_string: Annotated[str, Field(default="{value}")]

        transformer: Literal["strings.format"]
        config: Config

    class Hash(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[Optional[str], Field(default=None)]
            inFields: Annotated[list[str], Field(default=[])]
            outField: Annotated[str, Field(min_length=1)]
            hashWhen: Annotated[
                Literal["always", "any-value-is-populated", "all-values-are-populated"],
                Field(default="always"),
            ]

            @model_validator(mode="after")
            def validate_fields(cls, model: "StringTransformerSchema.Hash.Config"):
                if not model.inField and not model.inFields:
                    raise ValueError("Either inField or inFields must be provided")
                if len(model.inField or "") > 0 and len(model.inFields) > 0:
                    raise ValueError("Only one of inField or inFields can be provided")
                return model

        transformer: Literal["strings.hash"]
        config: Config


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
    StringTransformerSchema.Format,
    StringTransformerSchema.Hash,
]
