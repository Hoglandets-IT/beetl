from typing import Annotated, Literal, Optional, Union

from pydantic import Field

from .interface import TransformerConfigBase, TransformerSchemaBase


class IntegerTransformerSchema:
    class Divide(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[str, Field(min_length=1)]
            factor: int
            inType: Annotated[str, Field(default="Int64")]
            outType: Annotated[str, Field(default="Int32")]

        transformer: Literal["int.divide"]
        config: Config

    class Fillna(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[Optional[str], Field(default=None)]
            value: Annotated[int, Field(default=0)]

        transformer: Literal["int.fillna"]
        config: Config

    class ToInt64(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[Optional[str], Field(default=None)]

        transformer: Literal["int.to_int64"]
        config: Config


IntegerTransformerSchemas = Union[
    IntegerTransformerSchema.Divide,
    IntegerTransformerSchema.Fillna,
    IntegerTransformerSchema.ToInt64,
]
